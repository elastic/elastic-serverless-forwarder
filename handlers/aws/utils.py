# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import json
import os
from typing import Any, Callable

import boto3
from aws_lambda_typing import context as context_
from botocore.client import BaseClient as BotoBaseClient
from elasticapm import Client, get_client
from elasticapm.contrib.serverless.aws import capture_serverless as apm_capture_serverless  # noqa: F401

from share import shared_logger
from storage import CommonStorage, StorageFactory

_available_triggers: dict[str, str] = {"aws:sqs": "sqs"}


def get_sqs_client() -> BotoBaseClient:
    """
    Getter for sqs client
    Extracted for mocking
    """
    return boto3.client("sqs")


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
        except (ConfigFileException, InputConfigException, OutputConfigException, TriggerTypeException) as e:
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


def config_yaml_from_payload(lambda_event: dict[str, Any]) -> str:
    """
    Extract the config yaml from sqs record message attributes.
    In case we are in a sqs continuing handler scenario we use the config
    we set when sending the sqs continuing message instead of the one defined
    from env variable
    """

    payload = lambda_event["Records"][0]["messageAttributes"]
    config_yaml: str = payload["config"]["stringValue"]
    lambda_event["Records"][0]["eventSourceARN"] = payload["originalEventSource"]["stringValue"]

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


def get_trigger_type(event: dict[str, Any]) -> str:
    """
    Determines the trigger type according to the payload of the trigger event
    """

    if "Records" not in event or len(event["Records"]) < 1:
        raise Exception("Not supported trigger")

    if "body" in event["Records"][0]:
        event_body = event["Records"][0]["body"]
        if "output_type" in event_body and "output_args" in event_body and "payload" in event_body:
            return "replay"

    if "eventSource" not in event["Records"][0]:
        raise Exception("Not supported trigger")

    event_source = event["Records"][0]["eventSource"]
    if event_source not in _available_triggers:
        raise Exception("Not supported trigger")

    trigger_type = _available_triggers[event_source]
    if trigger_type != "sqs":
        return trigger_type

    if "messageAttributes" not in event["Records"][0]:
        return trigger_type

    if "originalEventSource" not in event["Records"][0]["messageAttributes"]:
        return trigger_type

    return "self_sqs"


def replay_handler(output_type: str, output_args: dict[str, Any], payload: dict[str, Any]) -> None:
    sqs_replay_queue = os.environ["SQS_REPLAY_URL"]

    sqs_client = get_sqs_client()

    sqs_client.send_message(
        QueueUrl=sqs_replay_queue,
        MessageBody=json.dumps({"output_type": output_type, "output_args": output_args, "payload": payload}),
    )

    shared_logger.warn(
        "sent to replay queue",
        extra={"sqs_replay_queue": sqs_replay_queue, "output_type": output_type, "payload": payload},
    )
