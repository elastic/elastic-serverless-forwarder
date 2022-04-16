# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import hashlib
import json
import os
from typing import Any, Callable, Optional

import boto3
from aws_lambda_typing import context as context_
from botocore.client import BaseClient as BotoBaseClient
from elasticapm import Client, get_client
from elasticapm.contrib.serverless.aws import capture_serverless as apm_capture_serverless  # noqa: F401

from share import Input, Output, shared_logger
from shippers import CompositeShipper, ElasticsearchShipper, ShipperFactory
from storage import CommonStorage, StorageFactory

_available_triggers: dict[str, str] = {"aws:s3": "s3-sqs", "aws:sqs": "sqs", "aws:kinesis": "kinesis-data-stream"}

CONFIG_FROM_PAYLOAD: str = "CONFIG_FROM_PAYLOAD"
CONFIG_FROM_S3FILE: str = "CONFIG_FROM_S3FILE"


def get_sqs_client() -> BotoBaseClient:
    """
    Getter for sqs client
    Extracted for mocking
    """
    return boto3.client("sqs")


def get_cloudwatch_logs_client() -> BotoBaseClient:
    """
    Getter for cloudwatch logs client
    Extracted for mocking
    """
    return boto3.client("logs")


def capture_serverless(
    func: Callable[[dict[str, Any], context_.Context], str]
) -> Callable[[dict[str, Any], context_.Context], str]:
    """
    Decorator with logic regarding when to inject apm_capture_serverless
    decorator: apm_capture_serverless expects handler to be run in a lambda
    and bew always active. We inject apm_capture_serverless decorator only if
    env variable ELASTIC_APM_ACTIVE is set and we are running in a real lambda:
    this allows us to run the handler locally or in different environment.
    """
    if "ELASTIC_APM_ACTIVE" not in os.environ or "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:

        def wrapper(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
            return func(lambda_event, lambda_context)

        return wrapper

    os.environ["ELASTIC_APM_COLLECT_LOCAL_VARIABLES"] = "off"
    return apm_capture_serverless()(func)  # type:ignore


class TriggerTypeException(Exception):
    """Raised when there is an error related to the trigger type"""

    pass


class ConfigFileException(Exception):
    """Raised when there is an error related to the config file"""

    pass


class InputConfigException(Exception):
    """Raised when there is an error related to the configured input"""

    pass


class OutputConfigException(Exception):
    """Raised when there is an error related to the configured output"""

    pass


class ReplayHandlerException(Exception):
    """Raised when there is an error in ingestion in the replay queue"""

    pass


def wrap_try_except(
    func: Callable[[dict[str, Any], context_.Context], str]
) -> Callable[[dict[str, Any], context_.Context], str]:
    """
    Decorator to catch every exception and capture them by apm client if set
    or raise if type is of between TriggerTypeException, ConfigFileException,
    InputConfigException or OutputConfigException
    """

    def wrapper(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
        apm_client: Client = get_client()
        try:
            return func(lambda_event, lambda_context)

        # NOTE: for all these cases we want the exception to bubble up to Lambda platform and let the defined retry
        # mechanism take action. These are non transient unrecoverable error from this code point of view.
        except (
            ConfigFileException,
            InputConfigException,
            OutputConfigException,
            TriggerTypeException,
            ReplayHandlerException,
        ) as e:
            if apm_client:
                apm_client.capture_exception()

            shared_logger.exception("exception raised", exc_info=e)

            raise e

        # NOTE: any generic exception is logged and suppressed to prevent the entire Lambda function to fail.
        # As Lambda can process multiple events, when within a Lambda execution only some event produce an Exception
        # it should not prevent all other events to be ingested.
        except Exception as e:
            if apm_client:
                apm_client.capture_exception()

            shared_logger.exception("exception raised", exc_info=e)

            return f"exception raised: {e.__repr__()}"

    return wrapper


def get_shipper_from_input(
    event_input: Input, lambda_event: dict[str, Any], at_record: int, config_yaml: str
) -> CompositeShipper:
    composite_shipper: CompositeShipper = CompositeShipper()

    for output_type in event_input.get_output_types():
        if output_type == "elasticsearch":
            shared_logger.info("setting ElasticSearch shipper")
            output: Optional[Output] = event_input.get_output_by_type("elasticsearch")
            if output is None:
                raise OutputConfigException("no available output for elasticsearch type")

            try:
                shipper: ElasticsearchShipper = ShipperFactory.create_from_output(
                    output_type="elasticsearch", output=output
                )
                shipper.discover_dataset(event=lambda_event, at_record=at_record)
                composite_shipper.add_shipper(shipper=shipper)
                replay_handler = ReplayEventHandler(config_yaml=config_yaml, event_input=event_input)
                composite_shipper.set_replay_handler(replay_handler=replay_handler.replay_handler)

                if event_input.type == "cloudwatch-logs":
                    composite_shipper.set_event_id_generator(event_id_generator=cloudwatch_logs_object_id)
                elif event_input.type == "sqs":
                    composite_shipper.set_event_id_generator(event_id_generator=sqs_object_id)
                elif event_input.type == "s3-sqs":
                    composite_shipper.set_event_id_generator(event_id_generator=s3_object_id)
                elif event_input.type == "kinesis-data-stream":
                    composite_shipper.set_event_id_generator(event_id_generator=kinesis_record_id)
            except Exception as e:
                raise OutputConfigException(e)

    composite_shipper.add_include_exclude_filter(event_input.include_exclude_filter)

    return composite_shipper


def config_yaml_from_payload(lambda_event: dict[str, Any]) -> str:
    """
    Extract the config yaml from sqs record message attributes.
    In case we are in a sqs continuing handler scenario we use the config
    we set when sending the sqs continuing message instead of the one defined
    from env variable
    """

    payload = lambda_event["Records"][0]["messageAttributes"]
    config_yaml: str = payload["config"]["stringValue"]

    return config_yaml


def config_yaml_from_s3() -> str:
    """
    Extract the config yaml downloading it from S3
    It is the default behaviour: reference to the config file is given
    by env variable S3_CONFIG_FILE
    """

    config_file = os.getenv("S3_CONFIG_FILE")
    assert config_file is not None

    bucket_name, object_key = from_s3_uri_to_bucket_name_and_object_key(config_file)
    shared_logger.info("config file", extra={"bucket_name": bucket_name, "object_key": object_key})

    config_storage: CommonStorage = StorageFactory.create(
        storage_type="s3", bucket_name=bucket_name, object_key=object_key
    )

    config_yaml: str = config_storage.get_as_string()
    return config_yaml


def from_s3_uri_to_bucket_name_and_object_key(s3_uri: str) -> tuple[str, str]:
    """
    Helpers for extracting bucket name and object key given an S3 URI
    """

    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid s3 uri provided: `{s3_uri}`")

    stripped_s3_uri = s3_uri.strip("s3://")

    bucket_name_and_object_key = stripped_s3_uri.split("/", 1)
    if len(bucket_name_and_object_key) < 2:
        raise ValueError(f"Invalid s3 uri provided: `{s3_uri}`")

    return bucket_name_and_object_key[0], "/".join(bucket_name_and_object_key[1:])


def get_bucket_name_from_arn(bucket_arn: str) -> str:
    """
    Helpers for extracting bucket name from a bucket ARN
    """

    return bucket_arn.split(":")[-1]


def get_kinesis_stream_name_type_and_region_from_arn(kinesis_stream_arn: str) -> tuple[str, str, str]:
    """
    Helpers for extracting stream name and type and region from a kinesis stream ARN
    """

    arn_components = kinesis_stream_arn.split(":")
    stream_componets = arn_components[-1].split("/")
    return stream_componets[0], stream_componets[1], arn_components[3]


def get_sqs_queue_name_and_region_from_arn(sqs_queue_arn: str) -> tuple[str, str]:
    """
    Helpers for extracting queue name and region from a sqs queue ARN
    """

    arn_components = sqs_queue_arn.split(":")
    return arn_components[-1], arn_components[3]


def is_continuing_of_cloudwatch_logs(sqs_record: dict[str, Any]) -> bool:
    """
    Determines if the event is the continue queue payload of a cloudwatch logs
    """

    if "messageAttributes" not in sqs_record:
        return False

    if "originalEventSourceARN" not in sqs_record["messageAttributes"]:
        return False

    original_event_source: str = sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

    return original_event_source.startswith("arn:aws:logs")


def get_trigger_type_and_config_source(event: dict[str, Any]) -> tuple[str, str]:
    """
    Determines the trigger type according to the payload of the trigger event
    and if the config must be read from attributes or from S3 file in env
    """

    if "event" in event and "awslogs" in event["event"] and "data" in event["event"]["awslogs"]:
        return "cloudwatch-logs", CONFIG_FROM_S3FILE

    if "Records" not in event or len(event["Records"]) < 1:
        raise Exception("Not supported trigger")

    event_source = ""
    first_record = event["Records"][0]
    if "body" in first_record:
        event_body = first_record["body"]
        try:
            body = json.loads(event_body)
            if (
                (body, dict)
                and "output_type" in event_body
                and "output_args" in event_body
                and "event_payload" in event_body
            ):
                return "replay-sqs", CONFIG_FROM_PAYLOAD

            if (
                isinstance(body, dict)
                and "Records" in body
                and len(body["Records"]) > 0
                and "eventSource" in body["Records"][0]
            ):
                event_source = body["Records"][0]["eventSource"]
        except Exception:
            if "eventSource" not in first_record:
                raise Exception("Not supported trigger")

            event_source = first_record["eventSource"]
    else:
        if "eventSource" not in first_record:
            raise Exception("Not supported trigger")

        event_source = first_record["eventSource"]

    if event_source not in _available_triggers:
        raise Exception("Not supported trigger")

    trigger_type: Optional[str] = _available_triggers[event_source]
    assert trigger_type is not None

    if (
        trigger_type == "kinesis-data-stream"
        and "kinesis" not in first_record
        and "data" not in first_record["kinesis"]
    ):
        raise Exception("Not supported trigger")

    if "messageAttributes" not in first_record:
        return trigger_type, CONFIG_FROM_S3FILE

    if "originalEventSourceARN" not in first_record["messageAttributes"]:
        return trigger_type, CONFIG_FROM_S3FILE

    return trigger_type, CONFIG_FROM_PAYLOAD


class ReplayEventHandler:
    def __init__(self, config_yaml: str, event_input: Input):
        self._config_yaml: str = config_yaml
        self._event_input_id: str = event_input.id
        self._event_input_type: str = event_input.type

    def replay_handler(self, output_type: str, output_args: dict[str, Any], event_payload: dict[str, Any]) -> None:
        sqs_replay_queue = os.environ["SQS_REPLAY_URL"]

        sqs_client = get_sqs_client()

        message_payload: dict[str, Any] = {
            "output_type": output_type,
            "output_args": output_args,
            "event_payload": event_payload,
            "event_input_id": self._event_input_id,
            "event_input_type": self._event_input_type,
        }

        sqs_client.send_message(
            QueueUrl=sqs_replay_queue,
            MessageBody=json.dumps(message_payload),
            MessageAttributes={
                "config": {"StringValue": self._config_yaml, "DataType": "String"},
            },
        )

        shared_logger.warning("sent to replay queue", extra=message_payload)


def get_queue_url_from_sqs_arn(sqs_arn: str) -> str:
    """
    Return sqs queue url given an sqs queue arn
    """
    arn_components = sqs_arn.split(":")
    account_id = arn_components[4]
    queue_name = arn_components[5]

    sqs_client = get_sqs_client()

    queue_info = sqs_client.get_queue_url(QueueName=queue_name, QueueOwnerAWSAccountId=account_id)
    assert isinstance(queue_info["QueueUrl"], str)

    return queue_info["QueueUrl"]


def get_log_group_arn_and_region_from_log_group_name(log_group_name: str) -> tuple[str, str]:
    """
    Return cloudwatch log group arn given a log group name
    """
    logs_client = get_cloudwatch_logs_client()

    log_groups = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)

    assert "logGroups" in log_groups

    for log_group in log_groups["logGroups"]:
        if "logGroupName" in log_group and log_group["logGroupName"] == log_group_name:
            log_group_arn = log_group["arn"]
            region = log_group_arn.split(":")[3]

            return log_group_arn, region

    raise ValueError("Cannot find cloudwatch log group ARN")


def delete_sqs_record(sqs_arn: str, receipt_handle: str) -> None:
    """
    Sqs records can be batched, we should delete the successful one:
    otherwise if a failure happens the whole batch will go back to the queue
    """
    queue_url = get_queue_url_from_sqs_arn(sqs_arn)

    sqs_client = get_sqs_client()

    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    shared_logger.info("delete processed sqs message", extra={"queue_url": queue_url})


def s3_object_id(event_payload: dict[str, Any]) -> str:
    """
    Port of
    https://github.com/elastic/beats/blob/21dca31b6296736fa90fae39bff71f063522420f/x-pack/filebeat/input/awss3/s3_objects.go#L364-L371
    https://github.com/elastic/beats/blob/21dca31b6296736fa90fae39bff71f063522420f/x-pack/filebeat/input/awss3/s3_objects.go#L356-L358
    """
    offset: int = event_payload["fields"]["log"]["offset"]
    bucket_arn: str = event_payload["fields"]["aws"]["s3"]["bucket"]["arn"]
    object_key: str = event_payload["fields"]["aws"]["s3"]["object"]["key"]

    src: str = f"{bucket_arn}{object_key}"
    hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

    return f"{hex_prefix}-{offset:012d}"


def cloudwatch_logs_object_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from an sqs queue
    """

    offset: int = event_payload["fields"]["log"]["offset"]
    group_name: str = event_payload["fields"]["aws"]["awscloudwatch"]["log_group"]
    stream_name: str = event_payload["fields"]["aws"]["awscloudwatch"]["log_stream"]
    event_id: str = event_payload["fields"]["aws"]["awscloudwatch"]["event_id"]

    src: str = f"{group_name}{stream_name}{event_id}"
    hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

    return f"{hex_prefix}-{offset:012d}"


def sqs_object_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from an sqs queue
    """

    offset: int = event_payload["fields"]["log"]["offset"]
    queue_name: str = event_payload["fields"]["aws"]["sqs"]["name"]
    message_id: str = event_payload["fields"]["aws"]["sqs"]["message_id"]

    src: str = f"{queue_name}{message_id}"
    hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

    return f"{hex_prefix}-{offset:012d}"


def kinesis_record_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from a kinesis stream
    """
    offset: int = event_payload["fields"]["log"]["offset"]
    stream_type: str = event_payload["fields"]["aws"]["kinesis"]["type"]
    stream_name: str = event_payload["fields"]["aws"]["kinesis"]["name"]
    sequence_number: str = event_payload["fields"]["aws"]["kinesis"]["sequence_number"]

    src: str = f"{stream_type}{stream_name}{sequence_number}"
    hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

    return f"{hex_prefix}-{offset:012d}"
