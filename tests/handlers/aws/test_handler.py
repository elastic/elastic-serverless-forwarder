# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import datetime
import gzip
import hashlib
import json
import os
import random
import string
import time
from copy import deepcopy
from typing import Any, Callable, Optional, Union
from unittest import TestCase

import docker
import localstack.utils.aws.aws_stack
import mock
import pytest
from botocore.client import BaseClient as BotoBaseClient
from botocore.exceptions import ClientError
from docker.models.containers import Container
from elasticsearch import Elasticsearch
from localstack.utils import testutil
from localstack.utils.aws import aws_stack

from handlers.aws.utils import ConfigFileException, TriggerTypeException
from main_aws import handler


class ContextMock:
    def __init__(self, remaining_time_in_millis: int = 0):
        self._remaining_time_in_millis = remaining_time_in_millis

    aws_request_id = "aws_request_id"
    invoked_function_arn = "invoked:function:arn:invoked:function:arn"

    def get_remaining_time_in_millis(self) -> int:
        return self._remaining_time_in_millis


class MockContent:
    SECRETS_MANAGER_MOCK_DATA: dict[str, dict[str, str]] = {
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets": {
            "type": "SecretString",
            "data": json.dumps(
                {
                    "url": "mock_elastic_url",
                    "username": "mock_elastic_username",
                    "password": "mock_elastic_password",
                    "empty": "",
                }
            ),
        },
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret": {
            "type": "SecretString",
            "data": "mock_plain_text_sqs_arn",
        },
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_byte": {
            "type": "SecretString",
            "data": b"i am not a string",  # type: ignore
        },
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_int": {
            "type": "SecretString",
            "data": 2021,  # type: ignore
        },
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:binary_secret": {
            "type": "SecretBinary",
            "data": "bW9ja19uZ2lueC5sb2c=",
        },
        "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:empty_secret": {"type": "SecretString", "data": ""},
    }

    @staticmethod
    def _get_aws_sm_client(region_name: str) -> mock.MagicMock:
        client = mock.Mock()
        client.get_secret_value = MockContent.get_secret_value
        return client

    @staticmethod
    def get_secret_value(SecretId: str) -> Optional[dict[str, Union[bytes, str]]]:
        secrets = MockContent.SECRETS_MANAGER_MOCK_DATA.get(SecretId)

        if secrets is None:
            raise ClientError(
                {
                    "Error": {
                        "Message": "Secrets Manager can't find the specified secret.",
                        "Code": "ResourceNotFoundException",
                    }
                },
                "GetSecretValue",
            )

        if secrets["type"] == "SecretBinary":
            return {"SecretBinary": base64.b64decode(secrets["data"])}
        elif secrets["type"] == "SecretString":
            return {"SecretString": secrets["data"]}

        return None


@pytest.mark.unit
class TestLambdaHandlerFailure(TestCase):
    @mock.patch("share.secretsmanager._get_aws_sm_client", new=MockContent._get_aws_sm_client)
    def test_lambda_handler_failure(self) -> None:
        dummy_event: dict[str, Any] = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs",
                },
            ]
        }

        event_with_config: dict[str, Any] = {
            "Records": [
                {
                    "messageAttributes": {
                        "config": {"stringValue": "ADD_CONFIG_STRING_HERE", "dataType": "String"},
                        "originalEventSourceARN": {
                            "stringValue": "dummy_aws_sqs",
                            "dataType": "String",
                        },
                    },
                    "md5OfBody": "randomhash",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs",
                    "awsRegion": "eu-central-1",
                }
            ]
        }

        with self.subTest("Invalid s3 uri"):
            with self.assertRaisesRegex(ConfigFileException, "Invalid s3 uri provided: ``"):
                os.environ["S3_CONFIG_FILE"] = ""
                ctx = ContextMock()

                handler(dummy_event, ctx)  # type:ignore
        with self.subTest("Invalid s3 uri no bucket and key"):
            with self.assertRaisesRegex(ConfigFileException, "Invalid s3 uri provided: `s3://`"):
                os.environ["S3_CONFIG_FILE"] = "s3://"
                ctx = ContextMock()

                handler(dummy_event, ctx)  # type:ignore

        with self.subTest("no Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event: dict[str, Any] = {}

                handler(event, ctx)  # type:ignore

        with self.subTest("empty Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {"Records": []}

                handler(event, ctx)  # type:ignore

        with self.subTest("no eventSource in Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {"Records": [{}]}

                handler(event, ctx)  # type:ignore

        with self.subTest("no valid eventSource in Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {"Records": [{"eventSource": "invalid"}]}

                handler(event, ctx)  # type:ignore

        with self.subTest("no eventSource in body Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {"Records": [{"body": ""}]}

                handler(event, ctx)  # type:ignore

        with self.subTest("no valid eventSource in body Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {"Records": [{"body": "", "eventSource": "invalid"}]}

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: arn format too long"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Invalid arn format: "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret:THIS:IS:INVALID",
            ):
                ctx = ContextMock()
                config_yml: str = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret:THIS:IS:INVALID"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """
                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty region"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Must be provided region in arn: " "arn:aws:secretsmanager::123-456-789:secret:plain_secret",
            ):
                ctx = ContextMock()
                # BEWARE region is empty at id
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager::123-456-789:secret:plain_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty secrets manager name"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Must be provided secrets manager name in arn: "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:",
            ):
                ctx = ContextMock()
                # BEWARE empty secrets manager name at id
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: cannot use both plain text and key/value pairs"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "You cannot have both plain text and json key for the same "
                "secret: arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username",
            ):
                ctx = ContextMock()
                # BEWARE using es_secrets plain text for elasticsearch_url and es_secrets:username for username
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secrets"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty secret key"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:: key must "
                "not be empty",
            ):
                ctx = ContextMock()
                # BEWARE empty key at elasticsearch_url
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: secret does not exist"):
            with self.assertRaisesRegex(
                ConfigFileException,
                r"An error occurred \(ResourceNotFoundException\) when calling "
                "the GetSecretValue operation: Secrets Manager can't find the specified secret.",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:DOES_NOT_EXIST"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty plain secret value"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:empty_secret: must "
                "not be empty",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:empty_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty key/value secret value"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:empty: must "
                "not be empty",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:empty"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: plain text used as key/value"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret:SHOULD_NOT_HAVE_A_KEY: "
                "expected to be keys/values pair",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret:SHOULD_NOT_HAVE_A_KEY"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: key does not exist in secret manager"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:I_DO_NOT_EXIST: "
                "key not found",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:I_DO_NOT_EXIST"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: plain text secret not str"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_byte: "
                "expected to be a string",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_byte"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: json TypeError risen"):
            with self.assertRaisesRegex(
                ConfigFileException,
                "the JSON object must be str, bytes or bytearray, not int while parsing "
                "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_int",
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret_not_str_int"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("tags not list"):
            with self.assertRaisesRegex(ConfigFileException, "Tags must be of type list"):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                        tags: "tag1"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore

        with self.subTest("each tag must be of type str"):
            with self.assertRaisesRegex(
                ConfigFileException, r"Each tag must be of type str, given: \['tag1', 2, 'tag3'\]"
            ):
                ctx = ContextMock()
                config_yml = """
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                        tags:
                          - "tag1"
                          - 2
                          - "tag3"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                              es_index_or_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(event_with_config)
                event["Records"][0]["messageAttributes"]["config"]["stringValue"] = config_yml

                handler(event, ctx)  # type:ignore


def _mock_awsclient(service_name: str, region_name: str = "") -> BotoBaseClient:
    if not region_name:
        return aws_stack.connect_to_service(service_name)

    return aws_stack.connect_to_service(service_name, region_name=region_name)


def _wait_for_container(container: Container, port: str) -> None:
    while port not in container.ports or len(container.ports[port]) == 0 or "HostPort" not in container.ports[port][0]:
        container.reload()
        time.sleep(1)


def _wait_for_localstack_service(wait_callback: Callable[[], None]) -> None:
    while True:
        try:
            wait_callback()
        except Exception:
            time.sleep(1)
        else:
            break


def _create_secrets(secret_name: str, secret_data: dict[str, str], localstack_host_port: str) -> Any:
    client = aws_stack.connect_to_service(
        "secretsmanager", region_name="eu-central-1", endpoint_url=f"http://localhost:{localstack_host_port}"
    )
    client.create_secret(Name=secret_name, SecretString=json.dumps(secret_data))

    return client.describe_secret(SecretId=secret_name)["ARN"]


def _upload_content_to_bucket(content: Union[bytes, str], content_type: str, bucket_name: str, key_name: str) -> None:
    client = aws_stack.connect_to_service("s3")

    client.create_bucket(Bucket=bucket_name, ACL="public-read-write")
    client.put_object(Bucket=bucket_name, Key=key_name, Body=content, ContentType=content_type)


def _event_from_sqs_message(queue_attributes: dict[str, Any], limit_max_number_of_messages: int = 2) -> dict[str, Any]:
    sqs_client = aws_stack.connect_to_service("sqs")
    messages = sqs_client.receive_message(
        QueueUrl=queue_attributes["QueueUrl"],
        MaxNumberOfMessages=limit_max_number_of_messages,
        MessageAttributeNames=["All"],
    )

    assert "Messages" in messages
    assert len(messages["Messages"]) == 1

    message = {}
    original_message = messages["Messages"][0]
    for key in original_message:
        new_value = deepcopy(original_message[key])
        camel_case_key = "".join([key[0].lower(), key[1:]])
        message[camel_case_key] = new_value

    if "messageAttributes" in message:
        for attribute in message["messageAttributes"]:
            new_attribute = deepcopy(message["messageAttributes"][attribute])
            for attribute_key in message["messageAttributes"][attribute]:
                camel_case_key = "".join([attribute_key[0].lower(), attribute_key[1:]])
                new_attribute[camel_case_key] = new_attribute[attribute_key]

            message["messageAttributes"][attribute] = new_attribute

    message["eventSource"] = "aws:sqs"
    message["eventSourceARN"] = queue_attributes["QueueArn"]

    return dict(Records=[message])


def _create_cloudwatch_logs_group_and_stream(group_name: str, stream_name: str) -> Any:
    logs_client = aws_stack.connect_to_service("logs")
    logs_client.create_log_group(logGroupName=group_name)
    logs_client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)

    return logs_client.describe_log_groups(logGroupNamePrefix=group_name)["logGroups"][0]


def _event_to_cloudwatch_logs(group_name: str, stream_name: str, message_body: str) -> None:
    now = int(datetime.datetime.utcnow().strftime("%s")) * 1000
    logs_client = aws_stack.connect_to_service("logs")
    logs_client.put_log_events(
        logGroupName=group_name,
        logStreamName=stream_name,
        logEvents=[{"timestamp": now, "message": message_body}],
    )


def _event_from_cloudwatch_logs(group_name: str, stream_name: str) -> tuple[dict[str, Any], str]:
    logs_client = aws_stack.connect_to_service("logs")
    events = logs_client.get_log_events(logGroupName=group_name, logStreamName=stream_name)

    assert "events" in events
    assert len(events["events"]) == 1

    event_id = "".join(random.choices(string.digits, k=56))
    data_json = json.dumps(
        {
            "messageType": "DATA_MESSAGE",
            "owner": "000000000000",
            "logGroup": group_name,
            "logStream": stream_name,
            "subscriptionFilters": ["a-subscription-filter"],
            "logEvents": [
                {
                    "id": event_id,
                    "timestamp": events["events"][0]["timestamp"],
                    "message": events["events"][0]["message"],
                }
            ],
        }
    )

    data_gzip = gzip.compress(data_json.encode("UTF-8"))
    data_base64encoded = base64.b64encode(data_gzip)

    return {"event": {"awslogs": {"data": data_base64encoded}}}, event_id


def _event_to_sqs_message(queue_attributes: dict[str, Any], message_body: str) -> None:
    sqs_client = aws_stack.connect_to_service("sqs")

    sqs_client.send_message(
        QueueUrl=queue_attributes["QueueUrl"],
        MessageBody=message_body,
    )


def _s3_event_to_sqs_message(queue_attributes: dict[str, Any], filenames: list[str]) -> None:
    sqs_client = aws_stack.connect_to_service("sqs")

    records = []
    for filename in filenames:
        records.append(
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "eu-central-1",
                "eventTime": "2021-09-08T18:34:25.042Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "test-bucket",
                    "bucket": {
                        "name": "test-bucket",
                        "arn": "arn:aws:s3:::test-bucket",
                    },
                    "object": {
                        "key": f"{filename}",
                    },
                },
            }
        )

    sqs_client.send_message(
        QueueUrl=queue_attributes["QueueUrl"],
        MessageBody=json.dumps({"Records": records}),
    )


@pytest.mark.integration
class TestLambdaHandlerSuccessMixedInput(TestCase):
    def setUp(self) -> None:
        docker_client = docker.from_env()
        localstack.utils.aws.aws_stack.BOTO_CLIENTS_CACHE = {}

        self._localstack_container = docker_client.containers.run(
            "localstack/localstack",
            detach=True,
            environment=["SERVICES=logs,s3,sqs,secretsmanager"],
            ports={"4566/tcp": None},
        )

        _wait_for_container(self._localstack_container, "4566/tcp")

        self._LOGS_BACKEND = os.environ.get("S3_BACKEND", "")
        self._S3_BACKEND = os.environ.get("S3_BACKEND", "")
        self._SQS_BACKEND = os.environ.get("SQS_BACKEND", "")
        self._SECRETSMANAGER_BACKEND = os.environ.get("SECRETSMANAGER_BACKEND", "")

        self._LOCALSTACK_HOST_PORT: str = self._localstack_container.ports["4566/tcp"][0]["HostPort"]

        os.environ["LOGS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["S3_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SQS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SECRETSMANAGER_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="logs").describe_log_groups)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="s3").list_buckets)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="sqs").list_queues)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="secretsmanager").list_secrets)

        self._ELASTIC_USER: str = "elastic"
        self._ELASTIC_PASSWORD: str = "password"

        self._secret_arn = _create_secrets(
            "es_secrets",
            {"username": self._ELASTIC_USER, "password": self._ELASTIC_PASSWORD},
            self._LOCALSTACK_HOST_PORT,
        )

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.16.3",
            detach=True,
            environment=[
                "ES_JAVA_OPTS=-Xms1g -Xmx1g",
                f"ELASTIC_PASSWORD={self._ELASTIC_PASSWORD}",
                "xpack.security.enabled=true",
                "discovery.type=single-node",
                "network.bind_host=0.0.0.0",
                "logger.org.elasticsearch=DEBUG",
            ],
            ports={"9200/tcp": None},
        )

        _wait_for_container(self._elastic_container, "9200/tcp")

        self._ES_HOST_PORT: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]

        self._es_client = Elasticsearch(
            hosts=[f"127.0.0.1:{self._ES_HOST_PORT}"],
            scheme="http",
            http_auth=(self._ELASTIC_USER, self._ELASTIC_PASSWORD),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
            raise_on_error=False,
            raise_on_exception=False,
        )

        while not self._es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self._es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

        self._source_cloudwatch_logs_group_info = _create_cloudwatch_logs_group_and_stream(
            group_name="source-group", stream_name="source-stream"
        )

        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")
        self._replay_queue_info = testutil.create_sqs_queue("replay-queue")
        self._source_s3_queue_info = testutil.create_sqs_queue("source-s3-sqs-queue")
        self._source_sqs_queue_info = testutil.create_sqs_queue("source-sqs-queue")

        self._config_yaml: str = f"""
        inputs:
          - type: "cloudwatch-logs"
            id: "{self._source_cloudwatch_logs_group_info["arn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
          - type: "s3-sqs"
            id: "{self._source_s3_queue_info["QueueArn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                  es_index_or_datastream_name: logs-generic-default
          - type: "sqs"
            id: "{self._source_sqs_queue_info["QueueArn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                """

        _upload_content_to_bucket(
            content=self._config_yaml,
            content_type="text/plain",
            bucket_name="config-bucket",
            key_name="folder/config.yaml",
        )

        self._first_log_entry: str = (
            "{\n"
            '   "@timestamp": "2021-12-28T11:33:08.160Z",\n'
            '   "log.level": "info",\n'
            '   "message": "trigger"\n'
            "}\n"
            "\n"
        )

        self._second_log_entry: str = (
            "{\n"
            '    "ecs": {\n'
            '        "version": "1.6.0"\n'
            "    },\n"
            '    "log": {\n'
            '        "logger": "root",\n'
            '        "origin": {\n'
            '            "file": {\n'
            '                "line": 30,\n'
            '                "name": "handler.py"\n'
            "            },\n"
            '            "function": "lambda_handler"\n'
            "        },\n"
            '        "original": "trigger"\n'
            "    }\n"
            "}\n"
            "\n"
        )

        self._third_log_entry: str = (
            "{\n" '    "another": "continuation",\n' '    "from": "the",\n' '    "continuing": "queue"\n' "}\n" "\n"
        )

        self._cloudwatch_log: str = self._first_log_entry + self._second_log_entry + self._third_log_entry
        self._first_s3_log: str = self._first_log_entry + self._second_log_entry
        self._second_s3_log: str = self._third_log_entry

        _upload_content_to_bucket(
            content=gzip.compress(self._first_s3_log.encode("UTF-8")),
            content_type="application/x-gzip",
            bucket_name="test-bucket",
            key_name="exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz",
        )

        _upload_content_to_bucket(
            content=gzip.compress(self._second_s3_log.encode("UTF-8")),
            content_type="application/x-gzip",
            bucket_name="test-bucket",
            key_name="exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000001.gz",
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = self._replay_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        os.environ["LOGS_BACKEND"] = self._LOGS_BACKEND
        os.environ["S3_BACKEND"] = self._S3_BACKEND
        os.environ["SQS_BACKEND"] = self._SQS_BACKEND
        os.environ["SECRETSMANAGER_BACKEND"] = self._SECRETSMANAGER_BACKEND

        del os.environ["S3_CONFIG_FILE"]
        del os.environ["SQS_CONTINUE_URL"]
        del os.environ["SQS_REPLAY_URL"]

        self._elastic_container.stop()
        self._elastic_container.remove()

        self._localstack_container.stop()
        self._localstack_container.remove()

    @mock.patch("handlers.aws.handler._completion_grace_period", 1)
    def test_lambda_handler_replay(self) -> None:
        first_filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        second_filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000001.gz"
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "handlers.aws.utils.get_cloudwatch_logs_client",
                        lambda: _mock_awsclient(service_name="logs"),
                    ):
                        with mock.patch(
                            "share.secretsmanager._get_aws_sm_client",
                            lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                        ):
                            _s3_event_to_sqs_message(
                                queue_attributes=self._source_s3_queue_info, filenames=[first_filename, second_filename]
                            )
                            event_s3 = _event_from_sqs_message(queue_attributes=self._source_s3_queue_info)

                            _event_to_sqs_message(
                                queue_attributes=self._source_sqs_queue_info, message_body=self._cloudwatch_log
                            )
                            event_sqs = _event_from_sqs_message(queue_attributes=self._source_sqs_queue_info)

                            message_id = event_sqs["Records"][0]["messageId"]
                            src: str = f"source-sqs-queue{message_id}"
                            hex_prefix_sqs = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                            _event_to_cloudwatch_logs(
                                group_name="source-group",
                                stream_name="source-stream",
                                message_body=self._cloudwatch_log,
                            )
                            event_cloudwatch_logs, event_id_cloudwatch_logs = _event_from_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream"
                            )

                            src = f"source-groupsource-stream{event_id_cloudwatch_logs}"
                            hex_prefix_cloudwatch_logs = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                            # Create an expected id for s3-sqs so that es.send will fail
                            self._es_client.index(
                                index="logs-generic-default",
                                op_type="create",
                                id="e69eaefedb-000000000000",
                                document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                            )

                            # Create an expected id so that es.send will fail
                            self._es_client.index(
                                index="logs-generic-default",
                                op_type="create",
                                id=f"{hex_prefix_sqs}-000000000000",
                                document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                            )

                            # Create an expected id for cloudwatch-logs so that es.send will fail
                            self._es_client.index(
                                index="logs-generic-default",
                                op_type="create",
                                id=f"{hex_prefix_cloudwatch_logs}-000000000000",
                                document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                            )

                            self._es_client.indices.refresh(index="logs-generic-default")

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={
                                    "ids": {
                                        "values": [
                                            "e69eaefedb-000000000097",
                                            f"{hex_prefix_sqs}-000000000097",
                                            f"{hex_prefix_cloudwatch_logs}-000000000097",
                                        ]
                                    }
                                },
                            )
                            assert res["hits"]["total"] == {"value": 0, "relation": "eq"}

                            ctx = ContextMock(remaining_time_in_millis=2)

                            first_call = handler(event_s3, ctx)  # type:ignore

                            assert first_call == "completed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["e69eaefedb-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{first_filename}"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{first_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["13bb9dbeab-000000000000"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {
                                    "path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{second_filename}"
                                },
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{second_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            second_call = handler(event_sqs, ctx)  # type:ignore

                            assert second_call == "completed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000398"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 398,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            third_call = handler(event_cloudwatch_logs, ctx)  # type:ignore

                            assert third_call == "completed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000398"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 398,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            first_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )
                            second_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )
                            third_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )

                            replayed_events = dict(
                                Records=[
                                    first_replayed_event["Records"][0],
                                    second_replayed_event["Records"][0],
                                    third_replayed_event["Records"][0],
                                ]
                            )

                            fourth_call = handler(replayed_events, ctx)  # type:ignore

                            assert fourth_call == "replayed"

                            self._es_client.indices.refresh(index="logs-generic-default")

                            # Remove the expected id for s3-sqs so that it can be replayed
                            self._es_client.delete_by_query(
                                index="logs-generic-default",
                                body={"query": {"match": {"_id": "e69eaefedb-000000000000"}}},
                            )

                            # Remove the expected id for sqs so that it can be replayed
                            self._es_client.delete_by_query(
                                index="logs-generic-default",
                                body={"query": {"match": {"_id": f"{hex_prefix_sqs}-000000000000"}}},
                            )

                            # Remove the expected id for cloudwatch logs so that it can be replayed
                            self._es_client.delete_by_query(
                                index="logs-generic-default",
                                body={"query": {"match": {"_id": f"{hex_prefix_cloudwatch_logs}-000000000000"}}},
                            )

                            self._es_client.indices.refresh(index="logs-generic-default")

                            # implicit wait for the message to be back on the queue
                            time.sleep(35)
                            fourth_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )

                            fifth_call = handler(fourth_replayed_event, ctx)  # type:ignore

                            assert fifth_call == "replayed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 7

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["e69eaefedb-000000000000"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{first_filename}"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{first_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            # implicit wait for the message to be back on the queue
                            time.sleep(35)
                            fifth_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )

                            sixth_call = handler(fifth_replayed_event, ctx)  # type:ignore

                            assert sixth_call == "replayed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 8

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000000"]}},
                            )
                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            # implicit wait for the message to be back on the queue
                            time.sleep(35)
                            sixth_replayed_event = _event_from_sqs_message(
                                queue_attributes=self._replay_queue_info, limit_max_number_of_messages=1
                            )

                            seventh_call = handler(sixth_replayed_event, ctx)  # type:ignore

                            assert seventh_call == "replayed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 9

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000000"]}},
                            )
                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

    @mock.patch("handlers.aws.handler._completion_grace_period", 1)
    def test_lambda_handler_continuing(self) -> None:
        first_filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        second_filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000001.gz"
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "handlers.aws.utils.get_cloudwatch_logs_client",
                        lambda: _mock_awsclient(service_name="logs"),
                    ):
                        with mock.patch(
                            "share.secretsmanager._get_aws_sm_client",
                            lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                        ):

                            ctx = ContextMock()

                            _s3_event_to_sqs_message(
                                queue_attributes=self._source_s3_queue_info, filenames=[first_filename, second_filename]
                            )
                            event_s3 = _event_from_sqs_message(queue_attributes=self._source_s3_queue_info)

                            _event_to_sqs_message(
                                queue_attributes=self._source_sqs_queue_info, message_body=self._cloudwatch_log
                            )
                            event_sqs = _event_from_sqs_message(queue_attributes=self._source_sqs_queue_info)

                            message_id = event_sqs["Records"][0]["messageId"]
                            src: str = f"source-sqs-queue{message_id}"
                            hex_prefix_sqs = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                            _event_to_cloudwatch_logs(
                                group_name="source-group",
                                stream_name="source-stream",
                                message_body=self._cloudwatch_log,
                            )
                            event_cloudwatch_logs, event_id_cloudwatch_logs = _event_from_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream"
                            )

                            src = f"source-groupsource-stream{event_id_cloudwatch_logs}"
                            hex_prefix_cloudwatch_logs = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                            first_call = handler(event_s3, ctx)  # type:ignore

                            assert first_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 1

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["e69eaefedb-000000000000"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{first_filename}"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{first_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            first_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info
                            )

                            second_call = handler(event_sqs, ctx)  # type:ignore

                            assert second_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 2

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000000"]}},
                            )
                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            second_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info
                            )

                            third_call = handler(event_cloudwatch_logs, ctx)  # type:ignore

                            assert third_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 3

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000000"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._first_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            third_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info
                            )

                            continued_events = dict(
                                Records=[
                                    first_continued_event["Records"][0],
                                    second_continued_event["Records"][0],
                                    third_continued_event["Records"][0],
                                ]
                            )

                            fourth_call = handler(continued_events, ctx)  # type:ignore

                            assert fourth_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 4

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["e69eaefedb-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{first_filename}"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{first_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            fourth_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info,
                                limit_max_number_of_messages=1,
                            )

                            fifth_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info,
                                limit_max_number_of_messages=1,
                            )

                            sixth_continued_event = _event_from_sqs_message(
                                queue_attributes=self._continuing_queue_info,
                                limit_max_number_of_messages=1,
                            )

                            continued_events = dict(
                                Records=[
                                    fourth_continued_event["Records"][0],
                                    fifth_continued_event["Records"][0],
                                    sixth_continued_event["Records"][0],
                                ]
                            )

                            ctx = ContextMock(remaining_time_in_millis=2)
                            fifth_call = handler(continued_events, ctx)  # type:ignore

                            assert fifth_call == "completed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 9

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000097"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._second_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 97,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": ["13bb9dbeab-000000000000"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {
                                    "path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{second_filename}"
                                },
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "s3": {
                                    "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                    "object": {"key": f"{second_filename}"},
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "eu-central-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_sqs}-000000000398"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 398,
                                "file": {"path": self._source_sqs_queue_info["QueueUrl"]},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "sqs": {
                                    "name": "source-sqs-queue",
                                    "message_id": message_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix_cloudwatch_logs}-000000000398"]}},
                            )

                            assert res["hits"]["hits"][0]["_source"]["message"] == self._third_log_entry.rstrip("\n")

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 398,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id_cloudwatch_logs,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]


@pytest.mark.integration
class TestLambdaHandlerSuccessKinesisDataStream(TestCase):
    def setUp(self) -> None:
        docker_client = docker.from_env()
        localstack.utils.aws.aws_stack.BOTO_CLIENTS_CACHE = {}

        self._localstack_container = docker_client.containers.run(
            "localstack/localstack",
            detach=True,
            environment=["SERVICES=s3,sqs,kinesis,secretsmanager"],
            ports={"4566/tcp": None},
        )

        _wait_for_container(self._localstack_container, "4566/tcp")

        self._KINESIS_BACKEND = os.environ.get("KINESIS_BACKEND", "")
        self._S3_BACKEND = os.environ.get("S3_BACKEND", "")
        self._SQS_BACKEND = os.environ.get("SQS_BACKEND", "")
        self._SECRETSMANAGER_BACKEND = os.environ.get("SECRETSMANAGER_BACKEND", "")

        self._LOCALSTACK_HOST_PORT: str = self._localstack_container.ports["4566/tcp"][0]["HostPort"]

        os.environ["KINESIS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["S3_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SQS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SECRETSMANAGER_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="kinesis").list_streams)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="s3").list_buckets)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="sqs").list_queues)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="secretsmanager").list_secrets)

        self._ELASTIC_USER: str = "elastic"
        self._ELASTIC_PASSWORD: str = "password"

        self._secret_arn = _create_secrets(
            "es_secrets",
            {"username": self._ELASTIC_USER, "password": self._ELASTIC_PASSWORD},
            self._LOCALSTACK_HOST_PORT,
        )

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.16.2",
            detach=True,
            environment=[
                "ES_JAVA_OPTS=-Xms1g -Xmx1g",
                f"ELASTIC_PASSWORD={self._ELASTIC_PASSWORD}",
                "xpack.security.enabled=true",
                "discovery.type=single-node",
                "network.bind_host=0.0.0.0",
                "logger.org.elasticsearch=DEBUG",
            ],
            ports={"9200/tcp": None},
        )

        _wait_for_container(self._elastic_container, "9200/tcp")

        self._ES_HOST_PORT: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]

        self._es_client = Elasticsearch(
            hosts=[f"127.0.0.1:{self._ES_HOST_PORT}"],
            scheme="http",
            http_auth=(self._ELASTIC_USER, self._ELASTIC_PASSWORD),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )

        while not self._es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self._es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

        self._kinesis_stream = aws_stack.create_kinesis_stream("source-kinesis")
        self._kinesis_client = aws_stack.connect_to_service("kinesis")

        self._source_kinesis_info = self._kinesis_client.describe_stream(StreamName=self._kinesis_stream.stream_name)
        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")
        self._replay_queue_info = testutil.create_sqs_queue("replay-queue")

        self._config_yaml: str = f"""
        inputs:
          - type: "kinesis-data-stream"
            id: "{self._source_kinesis_info["StreamDescription"]["StreamARN"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                """

        _upload_content_to_bucket(
            content=self._config_yaml,
            content_type="text/plain",
            bucket_name="config-bucket",
            key_name="folder/config.yaml",
        )

        kinesis_waiter = self._kinesis_client.get_waiter("stream_exists")
        while True:
            try:
                kinesis_waiter.wait(StreamName=self._kinesis_stream.stream_name)
            except Exception:
                time.sleep(1)
            else:
                break

        self._kinesis_client.put_records(
            Records=[
                {
                    "PartitionKey": "PartitionKey",
                    "Data": base64.b64encode(
                        b'{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}\n'
                        b'{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": 30, '
                        b'"name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}\n'
                    ),
                },
                {
                    "PartitionKey": "PartitionKey",
                    "Data": base64.b64encode(
                        b'{"@timestamp":"2022-02-02T12:40:45.690Z","log.level":"warning","message":"no namespace set '
                        b'in config: using `default`","ecs":{"version":"1.6.0"} }'
                    ),
                },
            ],
            StreamName=self._kinesis_stream.stream_name,
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = self._replay_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        os.environ["KINESIS_BACKEND"] = self._KINESIS_BACKEND
        os.environ["S3_BACKEND"] = self._S3_BACKEND
        os.environ["SQS_BACKEND"] = self._SQS_BACKEND

        del os.environ["S3_CONFIG_FILE"]
        del os.environ["SQS_CONTINUE_URL"]
        del os.environ["SQS_REPLAY_URL"]

        self._elastic_container.stop()
        self._elastic_container.remove()

        self._localstack_container.stop()
        self._localstack_container.remove()

    @staticmethod
    def _event_from_kinesis_records(records: dict[str, Any], stream_attribute: dict[str, Any]) -> dict[str, Any]:
        assert "Records" in records

        new_records: list[dict[str, Any]] = []
        for original_record in records["Records"]:
            kinesis_record = {}

            for key in original_record:
                new_value = deepcopy(original_record[key])
                camel_case_key = "".join([key[0].lower(), key[1:]])
                kinesis_record[camel_case_key] = new_value

            new_records.append(
                {
                    "kinesis": kinesis_record,
                    "eventSource": "aws:kinesis",
                    "eventSourceARN": stream_attribute["StreamDescription"]["StreamARN"],
                }
            )

        return dict(Records=new_records)

    def test_lambda_handler_kinesis(self) -> None:
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch(
                    "share.secretsmanager._get_aws_sm_client",
                    lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                ):

                    shards_paginator = self._kinesis_client.get_paginator("list_shards")
                    shards_available = [
                        shard
                        for shard in shards_paginator.paginate(
                            StreamName=self._kinesis_stream.stream_name,
                            ShardFilter={"Type": "FROM_TRIM_HORIZON", "Timestamp": datetime.datetime(2015, 1, 1)},
                            PaginationConfig={
                                "MaxItems": 1,
                                "PageSize": 1,
                            },
                        )
                    ]

                    assert len(shards_available) == 1 and len(shards_available[0]["Shards"]) == 1

                    shard_iterator = self._kinesis_client.get_shard_iterator(
                        StreamName=self._kinesis_stream.stream_name,
                        ShardId=shards_available[0]["Shards"][0]["ShardId"],
                        ShardIteratorType="TRIM_HORIZON",
                        Timestamp=datetime.datetime(2015, 1, 1),
                    )

                    records = self._kinesis_client.get_records(ShardIterator=shard_iterator["ShardIterator"], Limit=2)

                    ctx = ContextMock()
                    event = self._event_from_kinesis_records(
                        records=records, stream_attribute=self._source_kinesis_info
                    )

                    first_call = handler(event, ctx)  # type:ignore

                    assert first_call == {
                        "batchItemFailures": [{"itemIdentifier": event["Records"][1]["kinesis"]["sequenceNumber"]}]
                    }

                    self._es_client.indices.refresh(index="logs-generic-default")
                    assert self._es_client.count(index="logs-generic-default")["count"] == 2

                    res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                    assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                    assert (
                        res["hits"]["hits"][0]["_source"]["message"]
                        == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}'
                    )
                    assert res["hits"]["hits"][0]["_source"]["log"] == {
                        "offset": 0,
                        "file": {"path": self._source_kinesis_info["StreamDescription"]["StreamARN"]},
                    }
                    assert res["hits"]["hits"][0]["_source"]["aws"] == {
                        "kinesis": {
                            "type": "stream",
                            "name": self._kinesis_stream.stream_name,
                            "sequence_number": event["Records"][0]["kinesis"]["sequenceNumber"],
                        }
                    }
                    assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                        "provider": "aws",
                        "region": "us-east-1",
                    }

                    assert res["hits"]["hits"][0]["_source"]["tags"] == [
                        "forwarded",
                        "generic",
                        "tag1",
                        "tag2",
                        "tag3",
                    ]

                    assert (
                        res["hits"]["hits"][1]["_source"]["message"]
                        == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": 30, '
                        '"name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}'
                    )

                    assert res["hits"]["hits"][1]["_source"]["log"] == {
                        "offset": 86,
                        "file": {"path": self._source_kinesis_info["StreamDescription"]["StreamARN"]},
                    }
                    assert res["hits"]["hits"][0]["_source"]["aws"] == {
                        "kinesis": {
                            "type": "stream",
                            "name": self._kinesis_stream.stream_name,
                            "sequence_number": event["Records"][0]["kinesis"]["sequenceNumber"],
                        }
                    }
                    assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                        "provider": "aws",
                        "region": "us-east-1",
                    }

                    assert res["hits"]["hits"][1]["_source"]["tags"] == [
                        "forwarded",
                        "generic",
                        "tag1",
                        "tag2",
                        "tag3",
                    ]

                    event["Records"] = event["Records"][1:]
                    second_call = handler(event, ctx)  # type:ignore

                    assert second_call == {"batchItemFailures": []}

                    self._es_client.indices.refresh(index="logs-generic-default")
                    assert self._es_client.count(index="logs-generic-default")["count"] == 3

                    res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                    assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

                    assert (
                        res["hits"]["hits"][2]["_source"]["message"]
                        == '{"@timestamp":"2022-02-02T12:40:45.690Z","log.level":"warning","message":"no namespace set '
                        'in config: using `default`","ecs":{"version":"1.6.0"} }'
                    )

                    assert res["hits"]["hits"][2]["_source"]["log"] == {
                        "offset": 0,
                        "file": {"path": self._source_kinesis_info["StreamDescription"]["StreamARN"]},
                    }
                    assert res["hits"]["hits"][2]["_source"]["aws"] == {
                        "kinesis": {
                            "type": "stream",
                            "name": self._kinesis_stream.stream_name,
                            "sequence_number": event["Records"][0]["kinesis"]["sequenceNumber"],
                        }
                    }
                    assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                        "provider": "aws",
                        "region": "us-east-1",
                    }

                    assert res["hits"]["hits"][2]["_source"]["tags"] == [
                        "forwarded",
                        "generic",
                        "tag1",
                        "tag2",
                        "tag3",
                    ]

                    while "NextShardIterator" in records:
                        records = self._kinesis_client.get_records(ShardIterator=records["NextShardIterator"], Limit=2)
                        assert not records["Records"]
                        break


@pytest.mark.integration
class TestLambdaHandlerSuccessS3SQS(TestCase):
    @staticmethod
    def _event_to_sqs_message(queue_attributes: dict[str, Any], filename: str) -> None:
        sqs_client = aws_stack.connect_to_service("sqs")

        sqs_client.send_message(
            QueueUrl=queue_attributes["QueueUrl"],
            MessageBody=json.dumps(
                {
                    "Records": [
                        {
                            "eventVersion": "2.1",
                            "eventSource": "aws:s3",
                            "awsRegion": "eu-central-1",
                            "eventTime": "2021-09-08T18:34:25.042Z",
                            "eventName": "ObjectCreated:Put",
                            "s3": {
                                "s3SchemaVersion": "1.0",
                                "configurationId": "test-bucket",
                                "bucket": {
                                    "name": "test-bucket",
                                    "arn": "arn:aws:s3:::test-bucket",
                                },
                                "object": {
                                    "key": f"{filename}",
                                },
                            },
                        }
                    ]
                }
            ),
        )

    def setUp(self) -> None:
        docker_client = docker.from_env()
        localstack.utils.aws.aws_stack.BOTO_CLIENTS_CACHE = {}

        self._localstack_container = docker_client.containers.run(
            "localstack/localstack",
            detach=True,
            environment=["SERVICES=s3,sqs,secretsmanager"],
            ports={"4566/tcp": None},
        )

        _wait_for_container(self._localstack_container, "4566/tcp")

        self._S3_BACKEND = os.environ.get("S3_BACKEND", "")
        self._SQS_BACKEND = os.environ.get("SQS_BACKEND", "")
        self._SECRETSMANAGER_BACKEND = os.environ.get("SECRETSMANAGER_BACKEND", "")

        self._LOCALSTACK_HOST_PORT: str = self._localstack_container.ports["4566/tcp"][0]["HostPort"]

        os.environ["S3_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SQS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SECRETSMANAGER_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="s3").list_buckets)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="sqs").list_queues)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="secretsmanager").list_secrets)

        self._ELASTIC_USER: str = "elastic"
        self._ELASTIC_PASSWORD: str = "password"

        self._secret_arn = _create_secrets(
            "es_secrets",
            {"username": self._ELASTIC_USER, "password": self._ELASTIC_PASSWORD},
            self._LOCALSTACK_HOST_PORT,
        )

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.16.3",
            detach=True,
            environment=[
                "ES_JAVA_OPTS=-Xms1g -Xmx1g",
                f"ELASTIC_PASSWORD={self._ELASTIC_PASSWORD}",
                "xpack.security.enabled=true",
                "discovery.type=single-node",
                "network.bind_host=0.0.0.0",
                "logger.org.elasticsearch=DEBUG",
            ],
            ports={"9200/tcp": None},
        )

        _wait_for_container(self._elastic_container, "9200/tcp")

        self._ES_HOST_PORT: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]

        self._es_client = Elasticsearch(
            hosts=[f"127.0.0.1:{self._ES_HOST_PORT}"],
            scheme="http",
            http_auth=(self._ELASTIC_USER, self._ELASTIC_PASSWORD),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )

        while not self._es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self._es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

        self._source_queue_info = testutil.create_sqs_queue("source-queue")
        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")
        self._replay_queue_info = testutil.create_sqs_queue("replay-queue")

        self._config_yaml: str = f"""
        inputs:
          - type: "s3-sqs"
            id: "{self._source_queue_info["QueueArn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                """

        _upload_content_to_bucket(
            content=self._config_yaml,
            content_type="text/plain",
            bucket_name="config-bucket",
            key_name="folder/config.yaml",
        )

        cloudwatch_log: bytes = (
            b'{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}\n'
            b'{"ecs": {"version": "1.6.0"}, "log": {"logger": '
            b'"root", "origin": {"file": {"line": 30, "name": "handler.py"}, "function": "lambda_handler"}, '
            b'"original": "trigger"}}\n'
            b'{"another": "continuation", "from": "the", "continuing": "queue"}\n'
        )

        _upload_content_to_bucket(
            content=gzip.compress(cloudwatch_log),
            content_type="application/x-gzip",
            bucket_name="test-bucket",
            key_name="exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz",
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = self._replay_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        os.environ["S3_BACKEND"] = self._S3_BACKEND
        os.environ["SQS_BACKEND"] = self._SQS_BACKEND
        os.environ["SECRETSMANAGER_BACKEND"] = self._SECRETSMANAGER_BACKEND

        del os.environ["S3_CONFIG_FILE"]
        del os.environ["SQS_CONTINUE_URL"]
        del os.environ["SQS_REPLAY_URL"]

        self._elastic_container.stop()
        self._elastic_container.remove()

        self._localstack_container.stop()
        self._localstack_container.remove()

    @mock.patch("handlers.aws.handler._completion_grace_period", 1)
    def test_lambda_handler_replay(self) -> None:
        filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "share.secretsmanager._get_aws_sm_client",
                        lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                    ):

                        # Create an expected id so that es.send will fail
                        self._es_client.index(
                            index="logs-aws.cloudwatch_logs-default",
                            op_type="create",
                            id="e69eaefedb-000000000000",
                            document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                        )
                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")

                        ctx = ContextMock(remaining_time_in_millis=2)

                        self._event_to_sqs_message(queue_attributes=self._source_queue_info, filename=filename)
                        event = _event_from_sqs_message(queue_attributes=self._source_queue_info)

                        first_call = handler(event, ctx)  # type:ignore

                        assert first_call == "completed"

                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")

                        res = self._es_client.search(
                            index="logs-aws.cloudwatch_logs-default",
                            query={"ids": {"values": ["e69eaefedb-000000000086", "e69eaefedb-000000000252"]}},
                        )
                        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][0]["_source"]["message"]
                            == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": '
                            '30, "name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}'
                        )

                        assert res["hits"]["hits"][0]["_source"]["log"] == {
                            "offset": 86,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][0]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][0]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        assert (
                            res["hits"]["hits"][1]["_source"]["message"]
                            == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                        )

                        assert res["hits"]["hits"][1]["_source"]["log"] == {
                            "offset": 252,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][1]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][1]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                        second_call = handler(event, ctx)  # type:ignore

                        assert second_call == "replayed"

                        # Remove the expected id so that it can be replayed
                        self._es_client.delete_by_query(
                            index="logs-aws.cloudwatch_logs-default",
                            body={"query": {"match": {"_id": "e69eaefedb-000000000000"}}},
                        )
                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")

                        # implicit wait for the message to be back on the queue
                        time.sleep(35)
                        event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                        third_call = handler(event, ctx)  # type:ignore

                        assert third_call == "replayed"

                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")
                        assert self._es_client.count(index="logs-aws.cloudwatch_logs-default")["count"] == 3

                        res = self._es_client.search(index="logs-aws.cloudwatch_logs-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}
                        assert (
                            res["hits"]["hits"][2]["_source"]["message"]
                            == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}'
                        )
                        assert res["hits"]["hits"][2]["_source"]["log"] == {
                            "offset": 0,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][2]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][2]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

    def test_lambda_handler_continuing(self) -> None:
        filename: str = "exportedlogs/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "share.secretsmanager._get_aws_sm_client",
                        lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                    ):

                        ctx = ContextMock()
                        self._event_to_sqs_message(queue_attributes=self._source_queue_info, filename=filename)
                        event = _event_from_sqs_message(queue_attributes=self._source_queue_info)

                        first_call = handler(event, ctx)  # type:ignore

                        assert first_call == "continuing"

                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")
                        assert self._es_client.count(index="logs-aws.cloudwatch_logs-default")["count"] == 1

                        res = self._es_client.search(index="logs-aws.cloudwatch_logs-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}
                        assert (
                            res["hits"]["hits"][0]["_source"]["message"]
                            == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}'
                        )
                        assert res["hits"]["hits"][0]["_source"]["log"] == {
                            "offset": 0,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][0]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][0]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        second_call = handler(event, ctx)  # type:ignore

                        assert second_call == "continuing"

                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")
                        assert self._es_client.count(index="logs-aws.cloudwatch_logs-default")["count"] == 2

                        res = self._es_client.search(index="logs-aws.cloudwatch_logs-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][1]["_source"]["message"]
                            == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": '
                            '30, "name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}'
                        )

                        assert res["hits"]["hits"][1]["_source"]["log"] == {
                            "offset": 86,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][1]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][1]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        third_call = handler(event, ctx)  # type:ignore

                        assert third_call == "continuing"

                        self._es_client.indices.refresh(index="logs-aws.cloudwatch_logs-default")
                        assert self._es_client.count(index="logs-aws.cloudwatch_logs-default")["count"] == 3

                        res = self._es_client.search(index="logs-aws.cloudwatch_logs-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][2]["_source"]["message"]
                            == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                        )

                        assert res["hits"]["hits"][2]["_source"]["log"] == {
                            "offset": 252,
                            "file": {"path": f"https://test-bucket.s3.eu-central-1.amazonaws.com/{filename}"},
                        }
                        assert res["hits"]["hits"][2]["_source"]["aws"] == {
                            "s3": {
                                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                "object": {"key": f"{filename}"},
                            }
                        }
                        assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "eu-central-1",
                        }

                        assert res["hits"]["hits"][2]["_source"]["tags"] == [
                            "forwarded",
                            "aws-cloudwatch_logs",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        fourth_call = handler(event, ctx)  # type:ignore

                        assert fourth_call == "completed"


@pytest.mark.integration
class TestLambdaHandlerSuccessSQS(TestCase):
    @staticmethod
    def _event_to_sqs_message(queue_attributes: dict[str, Any], message_body: str) -> None:
        sqs_client = aws_stack.connect_to_service("sqs")

        sqs_client.send_message(
            QueueUrl=queue_attributes["QueueUrl"],
            MessageBody=message_body,
        )

    def setUp(self) -> None:
        docker_client = docker.from_env()
        localstack.utils.aws.aws_stack.BOTO_CLIENTS_CACHE = {}

        self._localstack_container = docker_client.containers.run(
            "localstack/localstack",
            detach=True,
            environment=["SERVICES=s3,sqs,secretsmanager"],
            ports={"4566/tcp": None},
        )

        _wait_for_container(self._localstack_container, "4566/tcp")

        self._S3_BACKEND = os.environ.get("S3_BACKEND", "")
        self._SQS_BACKEND = os.environ.get("SQS_BACKEND", "")
        self._SECRETSMANAGER_BACKEND = os.environ.get("SECRETSMANAGER_BACKEND", "")

        self._LOCALSTACK_HOST_PORT: str = self._localstack_container.ports["4566/tcp"][0]["HostPort"]

        os.environ["S3_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SQS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SECRETSMANAGER_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="s3").list_buckets)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="sqs").list_queues)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="secretsmanager").list_secrets)

        self._ELASTIC_USER: str = "elastic"
        self._ELASTIC_PASSWORD: str = "password"

        self._secret_arn = _create_secrets(
            "es_secrets",
            {"username": self._ELASTIC_USER, "password": self._ELASTIC_PASSWORD},
            self._LOCALSTACK_HOST_PORT,
        )

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.16.3",
            detach=True,
            environment=[
                "ES_JAVA_OPTS=-Xms1g -Xmx1g",
                f"ELASTIC_PASSWORD={self._ELASTIC_PASSWORD}",
                "xpack.security.enabled=true",
                "discovery.type=single-node",
                "network.bind_host=0.0.0.0",
                "logger.org.elasticsearch=DEBUG",
            ],
            ports={"9200/tcp": None},
        )

        _wait_for_container(self._elastic_container, "9200/tcp")

        self._ES_HOST_PORT: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]

        self._es_client = Elasticsearch(
            hosts=[f"127.0.0.1:{self._ES_HOST_PORT}"],
            scheme="http",
            http_auth=(self._ELASTIC_USER, self._ELASTIC_PASSWORD),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )

        while not self._es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self._es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

        self._source_queue_info = testutil.create_sqs_queue("source-queue")
        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")
        self._replay_queue_info = testutil.create_sqs_queue("replay-queue")

        self._config_yaml: str = f"""
        inputs:
          - type: "sqs"
            id: "{self._source_queue_info["QueueArn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                """

        _upload_content_to_bucket(
            content=self._config_yaml,
            content_type="text/plain",
            bucket_name="config-bucket",
            key_name="folder/config.yaml",
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = self._replay_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        os.environ["S3_BACKEND"] = self._S3_BACKEND
        os.environ["SQS_BACKEND"] = self._SQS_BACKEND
        os.environ["SECRETSMANAGER_BACKEND"] = self._SECRETSMANAGER_BACKEND

        del os.environ["S3_CONFIG_FILE"]
        del os.environ["SQS_CONTINUE_URL"]
        del os.environ["SQS_REPLAY_URL"]

        self._elastic_container.stop()
        self._elastic_container.remove()

        self._localstack_container.stop()
        self._localstack_container.remove()

    @mock.patch("handlers.aws.handler._completion_grace_period", 1)
    def test_lambda_handler_replay(self) -> None:
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "share.secretsmanager._get_aws_sm_client",
                        lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                    ):
                        cloudwatch_log: str = (
                            '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}\n'
                            '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": 30, '
                            '"name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}\n'
                            '{"another": "continuation", "from": "the", "continuing": "queue"}\n'
                        )

                        _event_to_sqs_message(queue_attributes=self._source_queue_info, message_body=cloudwatch_log)

                        event = _event_from_sqs_message(queue_attributes=self._source_queue_info)

                        message_id = event["Records"][0]["messageId"]
                        src: str = f"source-queue{message_id}"
                        hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                        # Create an expected id so that es.send will fail
                        self._es_client.index(
                            index="logs-generic-default",
                            op_type="create",
                            id=f"{hex_prefix}-000000000000",
                            document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                        )
                        self._es_client.indices.refresh(index="logs-generic-default")

                        ctx = ContextMock(remaining_time_in_millis=2)

                        first_call = handler(event, ctx)  # type:ignore

                        assert first_call == "completed"

                        self._es_client.indices.refresh(index="logs-generic-default")

                        res = self._es_client.search(
                            index="logs-generic-default",
                            query={"ids": {"values": [f"{hex_prefix}-000000000086", f"{hex_prefix}-000000000252"]}},
                        )
                        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][0]["_source"]["message"]
                            == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": '
                            '30, "name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}'
                        )

                        assert res["hits"]["hits"][0]["_source"]["log"] == {
                            "offset": 86,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][0]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": message_id,
                            }
                        }
                        assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][0]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        assert (
                            res["hits"]["hits"][1]["_source"]["message"]
                            == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                        )

                        assert res["hits"]["hits"][1]["_source"]["log"] == {
                            "offset": 252,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][1]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": message_id,
                            }
                        }
                        assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][1]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                        second_call = handler(event, ctx)  # type:ignore

                        assert second_call == "replayed"

                        # Remove the expected id so that it can be replayed
                        self._es_client.delete_by_query(
                            index="logs-generic-default",
                            body={"query": {"match": {"_id": f"{hex_prefix}-000000000000"}}},
                        )
                        self._es_client.indices.refresh(index="logs-generic-default")

                        # implicit wait for the message to be back on the queue
                        time.sleep(35)
                        event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                        third_call = handler(event, ctx)  # type:ignore

                        assert third_call == "replayed"

                        self._es_client.indices.refresh(index="logs-generic-default")
                        assert self._es_client.count(index="logs-generic-default")["count"] == 3

                        res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}
                        assert (
                            res["hits"]["hits"][2]["_source"]["message"]
                            == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}'
                        )
                        assert res["hits"]["hits"][2]["_source"]["log"] == {
                            "offset": 0,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][2]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": message_id,
                            }
                        }
                        assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][2]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

    def test_lambda_handler_continuing(self) -> None:
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "share.secretsmanager._get_aws_sm_client",
                        lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                    ):

                        ctx = ContextMock()

                        cloudwatch_log: str = (
                            '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}\n'
                            '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": 30, '
                            '"name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}\n'
                            '{"another": "continuation", "from": "the", "continuing": "queue"}\n'
                        )

                        self._event_to_sqs_message(
                            queue_attributes=self._source_queue_info, message_body=cloudwatch_log
                        )

                        event = _event_from_sqs_message(queue_attributes=self._source_queue_info)

                        first_call = handler(event, ctx)  # type:ignore

                        assert first_call == "continuing"

                        self._es_client.indices.refresh(index="logs-generic-default")
                        assert self._es_client.count(index="logs-generic-default")["count"] == 1

                        res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][0]["_source"]["message"]
                            == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": "trigger"}'
                        )
                        assert res["hits"]["hits"][0]["_source"]["log"] == {
                            "offset": 0,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][0]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": event["Records"][0]["messageId"],
                            }
                        }
                        assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][0]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        second_call = handler(event, ctx)  # type:ignore

                        assert second_call == "continuing"

                        self._es_client.indices.refresh(index="logs-generic-default")
                        assert self._es_client.count(index="logs-generic-default")["count"] == 2

                        res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][1]["_source"]["message"]
                            == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": {"line": '
                            '30, "name": "handler.py"}, "function": "lambda_handler"}, "original": "trigger"}}'
                        )

                        assert res["hits"]["hits"][1]["_source"]["log"] == {
                            "offset": 86,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][1]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": event["Records"][0]["messageAttributes"]["originalMessageId"][
                                    "stringValue"
                                ],
                            }
                        }
                        assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][1]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        third_call = handler(event, ctx)  # type:ignore

                        assert third_call == "continuing"

                        self._es_client.indices.refresh(index="logs-generic-default")
                        assert self._es_client.count(index="logs-generic-default")["count"] == 3

                        res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

                        assert (
                            res["hits"]["hits"][2]["_source"]["message"]
                            == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                        )

                        assert res["hits"]["hits"][2]["_source"]["log"] == {
                            "offset": 252,
                            "file": {"path": self._source_queue_info["QueueUrl"]},
                        }
                        assert res["hits"]["hits"][2]["_source"]["aws"] == {
                            "sqs": {
                                "name": "source-queue",
                                "message_id": event["Records"][0]["messageAttributes"]["originalMessageId"][
                                    "stringValue"
                                ],
                            }
                        }
                        assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                            "provider": "aws",
                            "region": "us-east-1",
                        }

                        assert res["hits"]["hits"][2]["_source"]["tags"] == [
                            "forwarded",
                            "generic",
                            "tag1",
                            "tag2",
                            "tag3",
                        ]

                        event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                        fourth_call = handler(event, ctx)  # type:ignore

                        assert fourth_call == "completed"


@pytest.mark.integration
class TestLambdaHandlerSuccessCloudWatchLogs(TestCase):
    @staticmethod
    def _create_cloudwatch_logs_group_and_stream(group_name: str, stream_name: str) -> Any:
        logs_client = aws_stack.connect_to_service("logs")
        logs_client.create_log_group(logGroupName=group_name)
        logs_client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)

        return logs_client.describe_log_groups(logGroupNamePrefix=group_name)["logGroups"][0]

    @staticmethod
    def _event_to_cloudwatch_logs(group_name: str, stream_name: str, message_body: str) -> None:
        now = int(datetime.datetime.utcnow().strftime("%s")) * 1000
        logs_client = aws_stack.connect_to_service("logs")
        logs_client.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=[{"timestamp": now, "message": message_body}],
        )

    @staticmethod
    def _event_from_cloudwatch_logs(group_name: str, stream_name: str) -> tuple[dict[str, Any], str]:
        logs_client = aws_stack.connect_to_service("logs")
        events = logs_client.get_log_events(logGroupName=group_name, logStreamName=stream_name)

        assert "events" in events
        assert len(events["events"]) == 1

        event_id = "".join(random.choices(string.digits, k=56))
        data_json = json.dumps(
            {
                "messageType": "DATA_MESSAGE",
                "owner": "000000000000",
                "logGroup": group_name,
                "logStream": stream_name,
                "subscriptionFilters": ["a-subscription-filter"],
                "logEvents": [
                    {
                        "id": event_id,
                        "timestamp": events["events"][0]["timestamp"],
                        "message": events["events"][0]["message"],
                    }
                ],
            }
        )

        data_gzip = gzip.compress(data_json.encode("UTF-8"))
        data_base64encoded = base64.b64encode(data_gzip)

        return {"event": {"awslogs": {"data": data_base64encoded}}}, event_id

    def setUp(self) -> None:
        docker_client = docker.from_env()
        localstack.utils.aws.aws_stack.BOTO_CLIENTS_CACHE = {}

        self._localstack_container = docker_client.containers.run(
            "localstack/localstack",
            detach=True,
            environment=["SERVICES=logs,s3,sqs,secretsmanager"],
            ports={"4566/tcp": None},
        )

        _wait_for_container(self._localstack_container, "4566/tcp")

        self._LOGS_BACKEND = os.environ.get("S3_BACKEND", "")
        self._S3_BACKEND = os.environ.get("S3_BACKEND", "")
        self._SQS_BACKEND = os.environ.get("SQS_BACKEND", "")
        self._SECRETSMANAGER_BACKEND = os.environ.get("SECRETSMANAGER_BACKEND", "")

        self._LOCALSTACK_HOST_PORT: str = self._localstack_container.ports["4566/tcp"][0]["HostPort"]

        os.environ["LOGS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["S3_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SQS_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"
        os.environ["SECRETSMANAGER_BACKEND"] = f"http://localhost:{self._LOCALSTACK_HOST_PORT}"

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="logs").describe_log_groups)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="s3").list_buckets)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="sqs").list_queues)

        _wait_for_localstack_service(aws_stack.connect_to_service(service_name="secretsmanager").list_secrets)

        self._ELASTIC_USER: str = "elastic"
        self._ELASTIC_PASSWORD: str = "password"

        self._secret_arn = _create_secrets(
            "es_secrets",
            {"username": self._ELASTIC_USER, "password": self._ELASTIC_PASSWORD},
            self._LOCALSTACK_HOST_PORT,
        )

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.16.3",
            detach=True,
            environment=[
                "ES_JAVA_OPTS=-Xms1g -Xmx1g",
                f"ELASTIC_PASSWORD={self._ELASTIC_PASSWORD}",
                "xpack.security.enabled=true",
                "discovery.type=single-node",
                "network.bind_host=0.0.0.0",
                "logger.org.elasticsearch=DEBUG",
            ],
            ports={"9200/tcp": None},
        )

        _wait_for_container(self._elastic_container, "9200/tcp")

        self._ES_HOST_PORT: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]

        self._es_client = Elasticsearch(
            hosts=[f"127.0.0.1:{self._ES_HOST_PORT}"],
            scheme="http",
            http_auth=(self._ELASTIC_USER, self._ELASTIC_PASSWORD),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )

        while not self._es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self._es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

        self._source_cloudwatch_logs_group_info = self._create_cloudwatch_logs_group_and_stream(
            group_name="source-group", stream_name="source-stream"
        )

        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")
        self._replay_queue_info = testutil.create_sqs_queue("replay-queue")

        self._config_yaml: str = f"""
        inputs:
          - type: "cloudwatch-logs"
            id: "{self._source_cloudwatch_logs_group_info["arn"]}"
            tags:
              - "tag1"
              - "tag2"
              - "tag3"
            outputs:
              - type: "elasticsearch"
                args:
                  elasticsearch_url: "http://127.0.0.1:{self._ES_HOST_PORT}"
                  username: "{self._secret_arn}:username"
                  password: "{self._secret_arn}:password"
                """

        _upload_content_to_bucket(
            content=self._config_yaml,
            content_type="text/plain",
            bucket_name="config-bucket",
            key_name="folder/config.yaml",
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = self._replay_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        os.environ["LOGS_BACKEND"] = self._LOGS_BACKEND
        os.environ["S3_BACKEND"] = self._S3_BACKEND
        os.environ["SQS_BACKEND"] = self._SQS_BACKEND
        os.environ["SECRETSMANAGER_BACKEND"] = self._SECRETSMANAGER_BACKEND

        del os.environ["S3_CONFIG_FILE"]
        del os.environ["SQS_CONTINUE_URL"]
        del os.environ["SQS_REPLAY_URL"]

        self._elastic_container.stop()
        self._elastic_container.remove()

        self._localstack_container.stop()
        self._localstack_container.remove()

    @mock.patch("handlers.aws.handler._completion_grace_period", 1)
    def test_lambda_handler_replay(self) -> None:
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch(
                        "handlers.aws.utils.get_cloudwatch_logs_client", lambda: _mock_awsclient(service_name="logs")
                    ):
                        with mock.patch(
                            "share.secretsmanager._get_aws_sm_client",
                            lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                        ):
                            cloudwatch_log: str = (
                                '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": '
                                '"trigger"}\n{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": '
                                '{"file": {"line": 30, "name": "handler.py"}, "function": "lambda_handler"}, '
                                '"original": "trigger"}}\n{"another": "continuation", "from": "the", '
                                '"continuing": "queue"}'
                            )

                            self._event_to_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream", message_body=cloudwatch_log
                            )

                            event, event_id = self._event_from_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream"
                            )

                            src: str = f"source-groupsource-stream{event_id}"
                            hex_prefix = hashlib.sha256(src.encode("UTF-8")).hexdigest()[:10]

                            # Create an expected id so that es.send will fail
                            self._es_client.index(
                                index="logs-generic-default",
                                op_type="create",
                                id=f"{hex_prefix}-000000000000",
                                document={"@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
                            )
                            self._es_client.indices.refresh(index="logs-generic-default")

                            ctx = ContextMock(remaining_time_in_millis=2)

                            first_call = handler(event, ctx)  # type:ignore

                            assert first_call == "completed"

                            self._es_client.indices.refresh(index="logs-generic-default")

                            res = self._es_client.search(
                                index="logs-generic-default",
                                query={"ids": {"values": [f"{hex_prefix}-000000000086", f"{hex_prefix}-000000000252"]}},
                            )
                            assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                            assert (
                                res["hits"]["hits"][0]["_source"]["message"]
                                == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": '
                                '{"line": 30, "name": "handler.py"}, "function": "lambda_handler"}, "original": '
                                '"trigger"}}'
                            )

                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 86,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            assert (
                                res["hits"]["hits"][1]["_source"]["message"]
                                == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                            )

                            assert res["hits"]["hits"][1]["_source"]["log"] == {
                                "offset": 252,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][1]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][1]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                            second_call = handler(event, ctx)  # type:ignore

                            assert second_call == "replayed"

                            # Remove the expected id so that it can be replayed
                            self._es_client.delete_by_query(
                                index="logs-generic-default",
                                body={"query": {"match": {"_id": f"{hex_prefix}-000000000000"}}},
                            )
                            self._es_client.indices.refresh(index="logs-generic-default")

                            # implicit wait for the message to be back on the queue
                            time.sleep(35)
                            event = _event_from_sqs_message(queue_attributes=self._replay_queue_info)
                            third_call = handler(event, ctx)  # type:ignore

                            assert third_call == "replayed"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 3

                            res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                            assert res["hits"]["total"] == {"value": 3, "relation": "eq"}
                            assert (
                                res["hits"]["hits"][2]["_source"]["message"]
                                == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": '
                                '"trigger"}'
                            )
                            assert res["hits"]["hits"][2]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][2]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][2]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

    def test_lambda_handler_continuing(self) -> None:
        with mock.patch("storage.S3Storage._s3_client", _mock_awsclient(service_name="s3")):
            with mock.patch(
                "handlers.aws.utils.get_cloudwatch_logs_client", lambda: _mock_awsclient(service_name="logs")
            ):
                with mock.patch("handlers.aws.handler.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                    with mock.patch("handlers.aws.utils.get_sqs_client", lambda: _mock_awsclient(service_name="sqs")):
                        with mock.patch(
                            "share.secretsmanager._get_aws_sm_client",
                            lambda region_name: _mock_awsclient(service_name="secretsmanager", region_name=region_name),
                        ):
                            ctx = ContextMock()

                            cloudwatch_log: str = (
                                '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": '
                                '"trigger"}\n{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": '
                                '{"file": {"line": 30, "name": "handler.py"}, "function": "lambda_handler"}, '
                                '"original": "trigger"}}\n{"another": "continuation", "from": "the", '
                                '"continuing": "queue"}'
                            )

                            _event_to_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream", message_body=cloudwatch_log
                            )

                            event, event_id = _event_from_cloudwatch_logs(
                                group_name="source-group", stream_name="source-stream"
                            )

                            first_call = handler(event, ctx)  # type:ignore

                            assert first_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 1

                            res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                            assert res["hits"]["total"] == {"value": 1, "relation": "eq"}
                            assert (
                                res["hits"]["hits"][0]["_source"]["message"]
                                == '{"@timestamp": "2021-12-28T11:33:08.160Z", "log.level": "info", "message": '
                                '"trigger"}'
                            )
                            assert res["hits"]["hits"][0]["_source"]["log"] == {
                                "offset": 0,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][0]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][0]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][0]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                            second_call = handler(event, ctx)  # type:ignore

                            assert second_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 2

                            res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                            assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

                            assert (
                                res["hits"]["hits"][1]["_source"]["message"]
                                == '{"ecs": {"version": "1.6.0"}, "log": {"logger": "root", "origin": {"file": '
                                '{"line": 30, "name": "handler.py"}, "function": "lambda_handler"}, '
                                '"original": "trigger"}}'
                            )

                            assert res["hits"]["hits"][1]["_source"]["log"] == {
                                "offset": 86,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][1]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][1]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][1]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                            second_call = handler(event, ctx)  # type:ignore

                            assert second_call == "continuing"

                            self._es_client.indices.refresh(index="logs-generic-default")
                            assert self._es_client.count(index="logs-generic-default")["count"] == 3

                            res = self._es_client.search(index="logs-generic-default", sort="_seq_no")
                            assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

                            assert (
                                res["hits"]["hits"][2]["_source"]["message"]
                                == '{"another": "continuation", "from": "the", "continuing": "queue"}'
                            )

                            assert res["hits"]["hits"][2]["_source"]["log"] == {
                                "offset": 252,
                                "file": {"path": "source-group/source-stream"},
                            }
                            assert res["hits"]["hits"][2]["_source"]["aws"] == {
                                "awscloudwatch": {
                                    "log_group": "source-group",
                                    "log_stream": "source-stream",
                                    "event_id": event_id,
                                }
                            }
                            assert res["hits"]["hits"][2]["_source"]["cloud"] == {
                                "provider": "aws",
                                "region": "us-east-1",
                            }

                            assert res["hits"]["hits"][2]["_source"]["tags"] == [
                                "forwarded",
                                "generic",
                                "tag1",
                                "tag2",
                                "tag3",
                            ]

                            event = _event_from_sqs_message(queue_attributes=self._continuing_queue_info)
                            fourth_call = handler(event, ctx)  # type:ignore

                            assert fourth_call == "completed"
