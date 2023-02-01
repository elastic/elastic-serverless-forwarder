# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import hashlib
import os
from dataclasses import dataclass
from typing import Any, Callable, Optional

import boto3
from aws_lambda_typing import context as context_
from botocore.client import BaseClient as BotoBaseClient
from elasticapm import Client
from elasticapm import get_client as get_apm_client
from elasticapm.contrib.serverless.aws import capture_serverless as apm_capture_serverless  # noqa: F401
from share import (
    Config,
    FunctionContext,
    Input,
    Output,
    get_hex_prefix,
    function_ended_telemetry,
    input_has_output_type_telemetry,
    json_dumper,
    json_parser,
    shared_logger,
)
from shippers import CompositeShipper, ProtocolShipper, ShipperFactory
from storage import ProtocolStorage, StorageFactory

from .exceptions import (
    ConfigFileException,
    InputConfigException,
    OutputConfigException,
    ReplayHandlerException,
    TriggerTypeException,
)

_available_triggers: dict[str, str] = {"aws:s3": "s3-sqs", "aws:sqs": "sqs", "aws:kinesis": "kinesis-data-stream"}

CONFIG_FROM_PAYLOAD: str = "CONFIG_FROM_PAYLOAD"
CONFIG_FROM_S3FILE: str = "CONFIG_FROM_S3FILE"

INTEGRATION_SCOPE_GENERIC: str = "generic"


def get_sqs_client() -> BotoBaseClient:
    """
    Getter for sqs client
    Extracted for mocking
    """
    return boto3.client("sqs")


def get_ec2_client() -> BotoBaseClient:
    """
    Getter for ec2 client
    Extracted for mocking
    """
    return boto3.client("ec2")


def capture_serverless(
    func: Callable[[dict[str, Any], context_.Context], str]
) -> Callable[[dict[str, Any], context_.Context], str]:
    """
    Decorator with logic regarding when to inject apm_capture_serverless
    decorator: apm_capture_serverless expects handler to be run in a lambda
    and bew always active. We inject apm_capture_serverless decorator only if
    env variable ELASTIC_APM_ACTIVE is set, and we are running in a real lambda:
    this allows us to run the handler locally or in different environment.
    """
    if "ELASTIC_APM_ACTIVE" not in os.environ or "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:

        def wrapper(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
            return func(lambda_event, lambda_context)

        return wrapper

    return apm_capture_serverless()(func=func)  # type:ignore


def wrap_try_except(
    func: Callable[[dict[str, Any], context_.Context], str]
) -> Callable[[dict[str, Any], context_.Context], str]:
    """
    Decorator to catch every exception and capture them by apm client if set
    or raise if type is of between TriggerTypeException, ConfigFileException,
    InputConfigException or OutputConfigException
    """

    def wrapper(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
        apm_client: Client = get_apm_client()
        try:
            return func(lambda_event, lambda_context)

        # NOTE: for all these cases we want the exception to bubble up to Lambda platform and let the defined retry
        # mechanism take action. These are non-transient unrecoverable error from this code point of view.
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

            function_ended_telemetry(exception_raised=True)

            raise e

        # NOTE: any generic exception is logged and suppressed to prevent the entire Lambda function to fail.
        # As Lambda can process multiple events, when within a Lambda execution only some event produce an Exception
        # it should not prevent all other events to be ingested.
        except Exception as e:
            if apm_client:
                apm_client.capture_exception()

            shared_logger.exception("exception raised", exc_info=e)

            function_ended_telemetry(exception_ignored=True)

            return f"exception raised: {e.__repr__()}"

    return wrapper


def discover_integration_scope(s3_object_key: str) -> str:
    if s3_object_key == "":
        shared_logger.debug("s3 object key is empty, dataset set to `generic`")
        return INTEGRATION_SCOPE_GENERIC
    else:
        if "/CloudTrail/" in s3_object_key or "/CloudTrail-Insight/" in s3_object_key:
            return "aws.cloudtrail"
        elif "/CloudTrail-Digest/" in s3_object_key:
            return "aws.cloudtrail-digest"
        elif "exportedlogs" in s3_object_key or "awslogs" in s3_object_key:
            return "aws.cloudwatch_logs"
        elif "/elasticloadbalancing/" in s3_object_key:
            return "aws.elb_logs"
        elif "/network-firewall/" in s3_object_key:
            return "aws.firewall_logs"
        elif "/vpcflowlogs/" in s3_object_key:
            return "aws.vpcflow"
        elif "/WAFLogs/" in s3_object_key:
            return "aws.waf"
        else:
            return INTEGRATION_SCOPE_GENERIC


def get_shipper_from_input(event_input: Input, config_yaml: str) -> CompositeShipper:
    composite_shipper: CompositeShipper = CompositeShipper()

    for output_type in event_input.get_output_types():
        anonymized_arn = anonymize_arn(event_input.id)
        input_has_output_type_telemetry(
            input_id=anonymized_arn.id,
            output_type=output_type,
        )

        if output_type == "elasticsearch":
            shared_logger.debug("setting ElasticSearch shipper")
            elasticsearch_output: Optional[Output] = event_input.get_output_by_type("elasticsearch")
            assert elasticsearch_output is not None

            elasticsearch_shipper: ProtocolShipper = ShipperFactory.create_from_output(
                output_type="elasticsearch", output=elasticsearch_output
            )
            composite_shipper.add_shipper(shipper=elasticsearch_shipper)

        if output_type == "logstash":
            shared_logger.debug("setting Logstash shipper")
            logstash_output: Optional[Output] = event_input.get_output_by_type("logstash")
            assert logstash_output is not None

            logstash_shipper: ProtocolShipper = ShipperFactory.create_from_output(
                output_type="logstash", output=logstash_output
            )

            composite_shipper.add_shipper(shipper=logstash_shipper)

        replay_handler = ReplayEventHandler(event_input=event_input)
        composite_shipper.set_replay_handler(replay_handler=replay_handler.replay_handler)

        if event_input.type == "cloudwatch-logs":
            composite_shipper.set_event_id_generator(event_id_generator=cloudwatch_logs_object_id)
        elif event_input.type == "sqs":
            composite_shipper.set_event_id_generator(event_id_generator=sqs_object_id)
        elif event_input.type == "s3-sqs":
            composite_shipper.set_event_id_generator(event_id_generator=s3_object_id)
        elif event_input.type == "kinesis-data-stream":
            composite_shipper.set_event_id_generator(event_id_generator=kinesis_record_id)

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

    config_storage: ProtocolStorage = StorageFactory.create(
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

    stripped_s3_uri = s3_uri.replace("s3://", "")

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
    stream_components = arn_components[-1].split("/")
    return stream_components[0], stream_components[1], arn_components[3]


def get_sqs_queue_name_and_region_from_arn(sqs_queue_arn: str) -> tuple[str, str]:
    """
    Helpers for extracting queue name and region from a sqs queue ARN
    """

    arn_components = sqs_queue_arn.split(":")
    return arn_components[-1], arn_components[3]


def get_continuing_original_input_type(sqs_record: dict[str, Any]) -> Optional[str]:
    """
    Determines the original input type of the continue queue payload
    """

    if "messageAttributes" not in sqs_record:
        return None

    if "originalEventSourceARN" not in sqs_record["messageAttributes"]:
        return None

    original_event_source: str = sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

    if original_event_source.startswith("arn:aws:logs"):
        return "cloudwatch-logs"

    if original_event_source.startswith("arn:aws:kinesis") and original_event_source.find(":stream/") > -1:
        return "kinesis-data-stream"

    return None


def get_trigger_type_and_config_source(event: dict[str, Any]) -> tuple[str, str]:
    """
    Determines the trigger type according to the payload of the trigger event
    and if the config must be read from attributes or from S3 file in env
    """

    if "awslogs" in event and "data" in event["awslogs"]:
        return "cloudwatch-logs", CONFIG_FROM_S3FILE

    if "Records" not in event or len(event["Records"]) < 1:
        raise Exception("Not supported trigger")

    event_source = ""
    first_record = event["Records"][0]
    if "body" in first_record:
        event_body = first_record["body"]
        try:
            body = json_parser(event_body)
            if (
                isinstance(body, dict)
                and "output_type" in event_body
                and "output_args" in event_body
                and "event_payload" in event_body
            ):
                return "replay-sqs", CONFIG_FROM_S3FILE

            if (
                isinstance(body, dict)
                and "Records" in body
                and len(body["Records"]) > 0
                and "eventSource" in body["Records"][0]
            ):
                event_source = body["Records"][0]["eventSource"]
            else:
                raise Exception
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

    if "messageAttributes" not in first_record:
        return trigger_type, CONFIG_FROM_S3FILE

    if "originalEventSourceARN" not in first_record["messageAttributes"]:
        return trigger_type, CONFIG_FROM_S3FILE

    return trigger_type, CONFIG_FROM_PAYLOAD


class ReplayEventHandler:
    def __init__(self, event_input: Input):
        self._event_input_id: str = event_input.id

    def replay_handler(self, output_type: str, output_args: dict[str, Any], event_payload: dict[str, Any]) -> None:
        sqs_replay_queue = os.environ["SQS_REPLAY_URL"]

        sqs_client = get_sqs_client()

        message_payload: dict[str, Any] = {
            "output_type": output_type,
            "output_args": output_args,
            "event_payload": event_payload,
            "event_input_id": self._event_input_id,
        }

        sqs_client.send_message(QueueUrl=sqs_replay_queue, MessageBody=json_dumper(message_payload))

        shared_logger.debug(
            "sent to replay queue", extra={"output_type": output_type, "event_input_id": self._event_input_id}
        )


def get_queue_url_from_sqs_arn(sqs_arn: str) -> str:
    """
    Return sqs queue url given an sqs queue arn
    """
    arn_components = sqs_arn.split(":")
    region = arn_components[3]
    account_id = arn_components[4]
    queue_name = arn_components[5]
    return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"


def get_account_id_from_arn(lambda_arn: str) -> str:
    arn_components = lambda_arn.split(":")

    return arn_components[4]


def get_input_from_log_group_subscription_data(
    config: Config, account_id: str, log_group_name: str, log_stream_name: str
) -> tuple[str, Optional[Input]]:
    """
    This function is not less resilient than the previous get_log_group_arn_and_region_from_log_group_name()
    We avoid to call the describe_log_streams on the logs' client, since we have no way to apply the proper
    throttling because we'd need to know the number of concurrent lambda running at the time of the call.
    In order to not hardcode the list of regions we rely on ec2 DescribeRegions - as much weird as it is - that I found
    no information about any kind of throttling. Weadd IAM permissions for it in deployment.
    """
    all_regions = get_ec2_client().describe_regions(AllRegions=True)
    assert "Regions" in all_regions
    for region_data in all_regions["Regions"]:
        region = region_data["RegionName"]
        log_stream_arn = f"arn:aws:logs:{region}:{account_id}:log-group:{log_group_name}:log-stream:{log_stream_name}"
        event_input = config.get_input_by_id(log_stream_arn)

        if event_input is not None:
            return log_stream_arn, event_input

        log_group_arn_components = log_stream_arn.split(":")
        log_group_arn = f"{':'.join(log_group_arn_components[:-2])}:*"
        event_input = config.get_input_by_id(log_group_arn)

        if event_input is not None:
            return log_group_arn, event_input

    return "", None


def delete_sqs_record(sqs_arn: str, receipt_handle: str) -> None:
    """
    Sqs records can be batched, we should delete the successful one:
    otherwise if a failure happens the whole batch will go back to the queue
    """
    queue_url = get_queue_url_from_sqs_arn(sqs_arn)

    sqs_client = get_sqs_client()

    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    shared_logger.debug("delete processed sqs message", extra={"queue_url": queue_url})


def s3_object_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from an s3 bucket
    """

    offset: int = event_payload["fields"]["log"]["offset"]
    bucket_arn: str = event_payload["fields"]["aws"]["s3"]["bucket"]["arn"]
    object_key: str = event_payload["fields"]["aws"]["s3"]["object"]["key"]
    event_time: int = event_payload["meta"]["event_time"]

    src: str = f"{bucket_arn}-{object_key}"
    hex_src = get_hex_prefix(src)

    return f"{event_time}-{hex_src}-{offset:012d}"


def cloudwatch_logs_object_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from an sqs queue
    """

    offset: int = event_payload["fields"]["log"]["offset"]
    group_name: str = event_payload["fields"]["aws"]["cloudwatch"]["log_group"]
    stream_name: str = event_payload["fields"]["aws"]["cloudwatch"]["log_stream"]
    event_id: str = event_payload["fields"]["aws"]["cloudwatch"]["event_id"]
    event_timestamp: int = event_payload["meta"]["event_timestamp"]

    src: str = f"{group_name}-{stream_name}-{event_id}"
    hex_src = get_hex_prefix(src)

    return f"{event_timestamp}-{hex_src}-{offset:012d}"


def sqs_object_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from an sqs queue
    """

    offset: int = event_payload["fields"]["log"]["offset"]
    queue_name: str = event_payload["fields"]["aws"]["sqs"]["name"]
    message_id: str = event_payload["fields"]["aws"]["sqs"]["message_id"]
    sent_timestamp: int = event_payload["meta"]["sent_timestamp"]

    src: str = f"{queue_name}-{message_id}"
    hex_src = get_hex_prefix(src)

    return f"{sent_timestamp}-{hex_src}-{offset:012d}"


def kinesis_record_id(event_payload: dict[str, Any]) -> str:
    """
    Generates a unique event id given the payload of an event from a kinesis stream
    """
    offset: int = event_payload["fields"]["log"]["offset"]
    stream_type: str = event_payload["fields"]["aws"]["kinesis"]["type"]
    stream_name: str = event_payload["fields"]["aws"]["kinesis"]["name"]
    partition_key: str = event_payload["fields"]["aws"]["kinesis"]["partition_key"]
    sequence_number: str = event_payload["fields"]["aws"]["kinesis"]["sequence_number"]
    approximate_arrival_timestamp: int = event_payload["meta"]["approximate_arrival_timestamp"]

    src: str = f"{stream_type}-{stream_name}-{partition_key}-{sequence_number}"
    hex_src = get_hex_prefix(src)

    return f"{approximate_arrival_timestamp}-{hex_src}-{offset:012d}"


# This is implementation specific to AWS and should not reside on share
def expand_event_list_from_field_resolver(integration_scope: str, field_to_expand_event_list_from: str) -> str:
    """
    Overrides custom provided `expand_event_list_from_field` setting in case of integration scope autodiscovery
    """
    if integration_scope == "aws.cloudtrail":
        field_to_expand_event_list_from = "Records"

    return field_to_expand_event_list_from


@dataclass
class ARN(object):
    """The Amazon Resource Name (ARN) of an AWS resource."""

    id: str
    service: str
    region: str
    account_id: str


def anonymize_arn(arn: str) -> ARN:
    """Anonymize an ARN by hashing the account ID and the resource ID."""
    arn_components = arn.split(":")

    hashed_arn = hashlib.sha256(arn.encode("utf-8")).hexdigest()[:10]
    service = arn_components[2]
    region = arn_components[3]
    account_id = hashlib.sha256(arn_components[4].encode("utf-8")).hexdigest()[:10]

    return ARN(hashed_arn, service, region, account_id)


def build_function_context(lambda_context: context_.Context) -> FunctionContext:
    """Create a FunctionContext from a Lambda context."""
    anonymized_arn = anonymize_arn(lambda_context.invoked_function_arn)

    return FunctionContext(
        anonymized_arn.id,
        lambda_context.function_version,
        lambda_context.aws_request_id,
        anonymized_arn.region,
        "aws",
        lambda_context.memory_limit_in_mb,
    )
