# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from typing import Any, Callable

from aws_lambda_typing import context as context_

from share import shared_logger
from storage import CommonStorage, StorageFactory

_available_triggers: dict[str, str] = {"aws:sqs": "sqs"}


def wrap_try_except(func: Callable[[dict[str, Any], context_.Context], str]) -> Callable[[Any], str]:
    def wrapper(*args: Any) -> str:
        try:
            return func(*args)
        except Exception as e:
            shared_logger.exception("exception raised", exc_info=e)
            return str(e)

    return wrapper


def config_yaml_from_payload(lambda_event: dict[str, Any]) -> str:
    payload = lambda_event["Records"][0]["messageAttributes"]
    config_yaml: str = payload["config"]["stringValue"]
    lambda_event["Records"][0]["eventSourceARN"] = payload["originalEventSource"]["stringValue"]

    return config_yaml


def config_yaml_from_s3() -> str:
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
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    s3_uri = s3_uri.strip("s3://")

    bucket_name_and_object_key = s3_uri.split("/", 1)
    if len(bucket_name_and_object_key) != 2:
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    return bucket_name_and_object_key[0], bucket_name_and_object_key[1]


def get_bucket_name_from_arn(bucket_arn: str) -> str:
    return bucket_arn.split(":")[-1]


def get_trigger_type(event: dict[str, Any]) -> str:
    if "Records" not in event and len(event["Records"]) < 1:
        raise Exception("Not supported trigger")

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
