# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import datetime
import importlib
import os
import sys
from copy import deepcopy
from io import BytesIO
from typing import Any, Optional, Union
from unittest import TestCase

import mock
import pytest
import pytest_httpserver
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from handlers.aws.exceptions import (
    ConfigFileException,
    InputConfigException,
    OutputConfigException,
    TriggerTypeException,
)
from main_aws import handler
from share import Input, json_dumper, json_parser, telemetry_init

from .utils import ContextMock


class MockContent:
    SECRETS_MANAGER_MOCK_DATA: dict[str, dict[str, str]] = {
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets": {
            "type": "SecretString",
            "data": json_dumper(
                {
                    "url": "mock_elastic_url",
                    "username": "mock_elastic_username",
                    "password": "mock_elastic_password",
                    "empty": "",
                }
            ),
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret": {
            "type": "SecretString",
            "data": "mock_plain_text_sqs_arn",
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte": {
            "type": "SecretString",
            "data": b"i am not a string",  # type:ignore
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int": {
            "type": "SecretString",
            "data": 2021,  # type:ignore
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:binary_secret": {
            "type": "SecretBinary",
            "data": "bW9ja19uZ2lueC5sb2c=",
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:empty_secret": {"type": "SecretString", "data": ""},
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


_now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
_dummy_lambda_event: dict[str, Any] = {
    "Records": [
        {
            "messageId": "dummy_message_id",
            "receiptHandle": "dummy_receipt_handle",
            "body": json_dumper(
                {
                    "Records": [
                        {
                            "eventVersion": "2.1",
                            "eventSource": "aws:s3",
                            "awsRegion": "eu-central-1",
                            "eventTime": _now,
                            "eventName": "ObjectCreated:Put",
                            "userIdentity": {"principalId": "dummy_principal_id"},
                            "requestParameters": {"sourceIPAddress": "dummy_source_ip_address"},
                            "responseElements": {
                                "x-amz-request-id": "dummy_request_id",
                                "x-amz-id-2": "dummy_request_id_2",
                            },
                            "s3": {
                                "s3SchemaVersion": "1.0",
                                "configurationId": "sqs_event",
                                "bucket": {
                                    "name": "dummy_bucket_name",
                                    "ownerIdentity": {"principalId": "dummy_principal_id"},
                                    "arn": "arn:aws:s3:::dummy_bucket_name",
                                },
                                "object": {
                                    "key": "file.log",
                                    "size": 27,
                                    "eTag": "",
                                    "sequencer": "",
                                },
                            },
                        }
                    ]
                }
            ),
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": _now,
                "SenderId": "dummy_sender_id",
                "ApproximateFirstReceiveTimestamp": _now,
            },
            "messageAttributes": {
                "config": {
                    "stringValue": "inputs:\n  - type: s3-sqs"
                    "\n    id: arn:aws:sqs:eu-central-1:123456789:sqs-queue\n    outputs:"
                    "\n      - type: elasticsearch\n        args:"
                    "\n          cloud_id: cloud_id:bG9jYWxob3N0OjkyMDAkMA==\n          api_key: api_key\n"
                },
                "originalEventSourceARN": {"stringValue": "arn:aws:sqs:eu-central-1:123456789:sqs-queue"},
                "originalLastEndingOffset": {"stringValue": "32"},
            },
            "md5OfBody": "dummy_hash",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789:s3-sqs-queue",
            "awsRegion": "eu-central-1",
        }
    ]
}


def _get_queue_url_mock(QueueName: str, QueueOwnerAWSAccountId: str) -> dict[str, Any]:
    return {"QueueUrl": ""}


def _send_message(QueueUrl: str, MessageBody: str, MessageAttributes: dict[str, Any]) -> None:
    pass


def _describe_regions(AllRegions: bool) -> dict[str, Any]:
    return {
        "Regions": [
            {
                "RegionName": "af-south-1",
            },
            {
                "RegionName": "ap-east-1",
            },
            {
                "RegionName": "ap-northeast-1",
            },
            {
                "RegionName": "ap-northeast-2",
            },
            {
                "RegionName": "ap-northeast-3",
            },
            {
                "RegionName": "ap-south-1",
            },
            {
                "RegionName": "ap-south-2",
            },
            {
                "RegionName": "ap-southeast-1",
            },
            {
                "RegionName": "ap-southeast-2",
            },
            {
                "RegionName": "ap-southeast-3",
            },
            {
                "RegionName": "ap-southeast-4",
            },
            {
                "RegionName": "ca-central-1",
            },
            {
                "RegionName": "eu-central-1",
            },
            {
                "RegionName": "eu-central-2",
            },
            {
                "RegionName": "eu-north-1",
            },
            {
                "RegionName": "eu-south-1",
            },
            {
                "RegionName": "eu-south-2",
            },
            {
                "RegionName": "eu-west-1",
            },
            {
                "RegionName": "eu-west-2",
            },
            {
                "RegionName": "eu-west-3",
            },
            {
                "RegionName": "me-central-1",
            },
            {
                "RegionName": "me-south-1",
            },
            {
                "RegionName": "sa-east-1",
            },
            {
                "RegionName": "us-east-1",
            },
            {
                "RegionName": "us-east-2",
            },
            {
                "RegionName": "us-gov-east-1",
            },
            {
                "RegionName": "us-gov-west-1",
            },
            {
                "RegionName": "us-west-1",
            },
            {
                "RegionName": "us-west-2",
            },
        ]
    }


_ec2_client_mock = mock.MagicMock()
_ec2_client_mock.describe_regions = _describe_regions

_sqs_client_mock = mock.MagicMock()
_sqs_client_mock.get_queue_url = _get_queue_url_mock
_sqs_client_mock.send_message = _send_message


_s3_client_mock = mock.MagicMock()


_s3_client_mock.config_content = (
    b"inputs:\n"
    b"  - type: s3-sqs\n"
    b"    id: arn:aws:sqs:eu-central-1:123456789:s3-sqs-queue\n"
    b"    outputs:\n"
    b"      - type: elasticsearch\n"
    b"        args:\n"
    b"          cloud_id: cloud_id:bG9jYWxob3N0OjkyMDAkMA==\n"
    b"          api_key: api_key\n"
    b"      - type: logstash\n"
    b"        args:\n"
    b"          logstash_url: logstash_url\n"
    b"  - type: cloudwatch-logs\n"
    b"    id: arn:aws:logs:eu-central-1:123456789:log-group:logGroup:log-stream:logStream\n"
    b"    outputs:\n"
    b"      - type: elasticsearch\n"
    b"        args:\n"
    b"          cloud_id: cloud_id:bG9jYWxob3N0OjkyMDAkMA==\n"
    b"          api_key: api_key\n"
    b"      - type: logstash\n"
    b"        args:\n"
    b"          logstash_url: logstash_url\n"
    b"  - type: sqs\n"
    b"    id: arn:aws:sqs:eu-central-1:123456789:sqs-queue\n"
    b"    outputs:\n"
    b"      - type: elasticsearch\n"
    b"        args:\n"
    b"          cloud_id: cloud_id:bG9jYWxob3N0OjkyMDAkMA==\n"
    b"          api_key: api_key\n"
    b"      - type: logstash\n"
    b"        args:\n"
    b"          logstash_url: logstash_url\n"
    b"  - type: dummy\n"
    b"    id: arn:aws:dummy:eu-central-1:123456789:input\n"
    b"    outputs:\n"
    b"      - type: elasticsearch\n"
    b"        args:\n"
    b"          cloud_id: cloud_id:bG9jYWxob3N0OjkyMDAkMA==\n"
    b"          api_key: api_key\n"
    b"      - type: logstash\n"
    b"        args:\n"
    b"          logstash_url: logstash_url\n"
    b"  - type: s3-sqs\n"
    b"    id: arn:aws:sqs:eu-central-1:123456789:s3-sqs-queue-with-dummy-output\n"
    b"    outputs:\n"
    b"      - type: output_type\n"
    b"        args:\n"
    b"          output_arg: output_arg"
)


def _head_object(Bucket: str, Key: str) -> dict[str, Any]:
    return {"ContentType": "ContentType", "ContentLength": 0}


def _get_object(Bucket: str, Key: str, Range: str) -> dict[str, Any]:
    content = _s3_client_mock.config_content
    content_body = BytesIO(content)
    content_length = len(content)
    return {"Body": StreamingBody(content_body, content_length), "ContentLength": content_length}


def _download_fileobj(Bucket: str, Key: str, Fileobj: BytesIO) -> None:
    if Key == "please raise":
        raise Exception("raised")


_s3_client_mock.head_object = _head_object
_s3_client_mock.download_fileobj = _download_fileobj
_s3_client_mock.get_object = _get_object


def _apm_capture_serverless() -> Any:
    def wrapper(func: Any) -> Any:
        def decorated(*args: Any, **kwds: Any) -> Any:
            return func(*args, **kwds)

        return decorated

    return wrapper


def reload_handlers_aws_handler() -> None:
    os.environ["ELASTIC_APM_ACTIVE"] = "ELASTIC_APM_ACTIVE"
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "AWS_LAMBDA_FUNCTION_NAME"

    from handlers.aws.utils import get_ec2_client, get_sqs_client

    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    _ = get_sqs_client()
    _ = get_ec2_client()

    mock.patch("handlers.aws.utils.get_sqs_client", lambda: _sqs_client_mock).start()
    mock.patch("handlers.aws.utils.get_ec2_client", lambda: _ec2_client_mock).start()

    handlers_aws_handler = sys.modules["handlers.aws.handler"]
    importlib.reload(handlers_aws_handler)


def revert_handlers_aws_handler() -> None:
    if "AWS_DEFAULT_REGION" in os.environ:
        del os.environ["AWS_DEFAULT_REGION"]

    if "ELASTIC_APM_ACTIVE" in os.environ:
        del os.environ["ELASTIC_APM_ACTIVE"]

    if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
        del os.environ["AWS_LAMBDA_FUNCTION_NAME"]

    handlers_aws_handler = sys.modules["handlers.aws.handler"]
    importlib.reload(handlers_aws_handler)


@pytest.mark.unit
class TestTelemetry:
    @mock.patch("share.config._available_output_types", new=["elasticsearch", "output_type"])
    @mock.patch(
        "share.config._available_input_types", new=["cloudwatch-logs", "s3-sqs", "sqs", "kinesis-data-stream", "dummy"]
    )
    @mock.patch("handlers.aws.handler.get_sqs_client", lambda: _sqs_client_mock)
    @mock.patch("storage.S3Storage._s3_client", _s3_client_mock)
    @mock.patch("handlers.aws.utils.apm_capture_serverless", _apm_capture_serverless)
    @mock.patch(
        "handlers.aws.utils._available_triggers",
        new={"aws:s3": "s3-sqs", "aws:sqs": "sqs", "aws:kinesis": "kinesis-data-stream", "dummy": "s3-sqs"},
    )
    @mock.patch("share.telemetry.is_telemetry_enabled", lambda: True)
    def test_lambda_telemetry(self, httpserver: pytest_httpserver.HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        reload_handlers_aws_handler()

        # Make sure the telemetry worker is running.
        telemetry_init()

        # The telemetry_endpoint variable is defined when the module is loaded,
        # so we need to patch this instead of the environment variable.
        with mock.patch("share.telemetry.get_telemetry_endpoint", lambda: httpserver.url_for("/v3/send/esf")):
            # Set up the expectaction for the telemetry request
            # to the telemetry endpoint.
            httpserver.expect_oneshot_request(
                "/v3/send/esf",
                method="POST",
                json={
                    "function_id": "80036aa038",
                    "function_version": "v0.0.0",
                    "execution_id": "6b76b62589",
                    "cloud_provider": "aws",
                    "cloud_region": "us-east-1",
                    "memory_limit_in_mb": "512",
                    "input": {"type": "s3-sqs", "outputs": ["elasticsearch"]},
                },
            ).respond_with_json({"status": "ok"})

            # We wait for `timeout` seconds that the
            # telemetry machinery sends the request to the
            # telemetry endpoint.
            #
            # If the request is not sent within `timeout` seconds,
            # the test will fail.
            with httpserver.wait(timeout=5):
                # Prepare the lambda environment
                ctx = ContextMock()
                os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
                lambda_event = deepcopy(_dummy_lambda_event)
                del lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"]

                # Run the lambda handler
                assert handler(lambda_event, ctx) == "completed"  # type:ignore


@pytest.mark.unit
class TestLambdaHandlerNoop(TestCase):
    @mock.patch("share.config._available_output_types", new=["elasticsearch", "logstash", "output_type"])
    @mock.patch(
        "share.config._available_input_types", new=["cloudwatch-logs", "s3-sqs", "sqs", "kinesis-data-stream", "dummy"]
    )
    @mock.patch("storage.S3Storage._s3_client", _s3_client_mock)
    @mock.patch("handlers.aws.utils.apm_capture_serverless", _apm_capture_serverless)
    @mock.patch(
        "handlers.aws.utils._available_triggers",
        new={"aws:s3": "s3-sqs", "aws:sqs": "sqs", "aws:kinesis": "kinesis-data-stream", "dummy": "s3-sqs"},
    )
    def test_lambda_handler_noop(self) -> None:
        reload_handlers_aws_handler()

        with self.subTest("no originalEventSourceARN in messageAttributes"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = deepcopy(_dummy_lambda_event)
            del lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"]
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("no input defined for cloudwatch_logs"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = {
                "awslogs": {
                    "data": json_dumper(
                        {"logGroup": "logGroup", "logStream": "logStream", "owner": "123456789", "logEvents": []}
                    )
                }
            }
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("output not elasticsearch from payload config"):
            with mock.patch(
                "handlers.aws.handler.get_shipper_for_replay_event",
                lambda config, output_type, output_args, event_input_id, replay_handler: None,
            ):
                ctx = ContextMock()
                event = {
                    "Records": [
                        {
                            "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789:replay-queue",
                            "receiptHandle": "receiptHandle",
                            "body": '{"output_type": "output_type", "output_args": {},'
                            '"event_input_id": "arn:aws:sqs:eu-central-1:123456789:s3-sqs-queue", '
                            '"event_payload": {"_id": "_id"}}',
                        }
                    ]
                }
                assert handler(event, ctx) == "replayed"  # type:ignore

        with self.subTest("no input defined for cloudwatch_logs in continuing queue"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = deepcopy(_dummy_lambda_event)
            lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"] = {
                "stringValue": "arn:aws:logs:eu-central-1:123456789:log-group:test-not-existing-esf-loggroup:*"
            }
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("no output type elasticsearch in continuing queue"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = deepcopy(_dummy_lambda_event)
            lambda_event["Records"][0][
                "eventSourceARN"
            ] = "arn:aws:sqs:eu-central-1:123456789:s3-sqs-queue-with-dummy-output"
            del lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"]
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("no input type for output type elasticsearch in continuing queue"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = deepcopy(_dummy_lambda_event)
            lambda_event["Records"][0]["eventSource"] = "dummy"
            lambda_event["Records"][0]["eventSourceARN"] = "arn:aws:dummy:eu-central-1:123456789:input"
            del lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"]
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("no input defined for kinesis-data-stream"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = {
                "Records": [
                    {
                        "eventSource": "aws:kinesis",
                        "kinesis": {"data": ""},
                        "eventSourceARN": "arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream",
                    }
                ]
            }
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("body is neither replay queue nor s3-sqs"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            os.environ["SQS_REPLAY_URL"] = "https://sqs.us-east-2.amazonaws.com/123456789012/replay_queue"
            os.environ["SQS_CONTINUE_URL"] = "https://sqs.us-east-2.amazonaws.com/123456789012/continue_queue"
            lambda_event = deepcopy(_dummy_lambda_event)
            lambda_event["Records"][0]["body"] = json_dumper({"Records": [{"key": "value"}]})
            lambda_event["Records"][0]["eventSourceARN"] = "arn:aws:sqs:eu-central-1:123456789:sqs-queue"
            del lambda_event["Records"][0]["messageAttributes"]["originalEventSourceARN"]
            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("raising cannot find cloudwatch_logs ARN"):
            ctx = ContextMock()
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            lambda_event = {
                "awslogs": {
                    "data": json_dumper(
                        {"logGroup": "logGroup", "logStream": "logStreamNotMatching", "owner": "owner", "logEvents": []}
                    )
                }
            }

            assert handler(lambda_event, ctx) == "completed"  # type:ignore

        with self.subTest("raising unexpected exception"):
            ctx = ContextMock()
            lambda_event = deepcopy(_dummy_lambda_event)
            lambda_event_body = json_parser(lambda_event["Records"][0]["body"])
            lambda_event_body["Records"][0]["s3"]["object"]["key"] = "please raise"

            lambda_event["Records"][0]["body"] = json_dumper(lambda_event_body)

            assert handler(lambda_event, ctx) == "exception raised: Exception('raised')"  # type:ignore

        with self.subTest("raising unexpected exception apm client not None"):
            with mock.patch("handlers.aws.utils.get_apm_client", lambda: mock.MagicMock()):
                ctx = ContextMock()
                lambda_event = deepcopy(_dummy_lambda_event)
                lambda_event_body = json_parser(lambda_event["Records"][0]["body"])
                lambda_event_body["Records"][0]["s3"]["object"]["key"] = "please raise"

                lambda_event["Records"][0]["body"] = json_dumper(lambda_event_body)

                assert handler(lambda_event, ctx) == "exception raised: Exception('raised')"  # type:ignore


@pytest.mark.unit
class TestLambdaHandlerFailure(TestCase):
    def setUp(self) -> None:
        revert_handlers_aws_handler()

    @mock.patch("share.config._available_output_types", new=["elasticsearch", "logstash", "output_type"])
    @mock.patch(
        "share.config._available_input_types", new=["cloudwatch-logs", "s3-sqs", "sqs", "kinesis-data-stream", "dummy"]
    )
    @mock.patch("share.secretsmanager._get_aws_sm_client", new=MockContent._get_aws_sm_client)
    @mock.patch("handlers.aws.utils.get_ec2_client", lambda: _ec2_client_mock)
    @mock.patch("handlers.aws.handler.get_sqs_client", lambda: _sqs_client_mock)
    @mock.patch("storage.S3Storage._s3_client", _s3_client_mock)
    def test_lambda_handler_failure(self) -> None:
        dummy_event: dict[str, Any] = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs",
                },
            ]
        }

        with self.subTest("output not in config from replay payload body"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            event = {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789:replay-queue",
                        "receiptHandle": "receiptHandle",
                        "body": '{"output_type": "output_type", "output_args": {},'
                        '"event_input_id": "arn:aws:dummy:eu-central-1:123456789:input", '
                        '"event_payload": {"_id": "_id"}}',
                    }
                ]
            }
            with self.assertRaisesRegex(OutputConfigException, "Cannot load output of type output_type"):
                ctx = ContextMock()

                handler(event, ctx)  # type:ignore

        with self.subTest("input not in config from replay payload body"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            event = {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789:replay-queue",
                        "receiptHandle": "receiptHandle",
                        "body": '{"output_type": "output_type", "output_args": {},'
                        '"event_input_id": "arn:aws:dummy:eu-central-1:123456789:not-existing-input", '
                        '"event_payload": {"_id": "_id"}}',
                    }
                ]
            }
            with self.assertRaisesRegex(
                InputConfigException,
                "Cannot load input for input id arn:aws:dummy:eu-central-1:123456789:not-existing-input",
            ):
                ctx = ContextMock()

                handler(event, ctx)  # type:ignore

        with self.subTest("empty config"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(ConfigFileException, "Empty config"):
                ctx = ContextMock()
                _s3_client_mock.config_content = b""

                handler(dummy_event, ctx)  # type:ignore

        with self.subTest("Invalid s3 uri apm client not None"):
            with mock.patch("handlers.aws.utils.get_apm_client", lambda: mock.MagicMock()):
                with self.assertRaisesRegex(ConfigFileException, "Invalid s3 uri provided: ``"):
                    os.environ["S3_CONFIG_FILE"] = ""
                    ctx = ContextMock()

                    handler(dummy_event, ctx)  # type:ignore

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

        with self.subTest("Invalid s3 uri no key"):
            with self.assertRaisesRegex(ConfigFileException, "Invalid s3 uri provided: `s3://bucket`"):
                os.environ["S3_CONFIG_FILE"] = "s3://bucket"
                ctx = ContextMock()

                handler(dummy_event, ctx)  # type:ignore

        with self.subTest("no Records in event"):
            with self.assertRaisesRegex(TriggerTypeException, "Not supported trigger"):
                ctx = ContextMock()
                event = {}

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

        with self.subTest("replay event loads config from s3"):
            with self.assertRaisesRegex(ConfigFileException, "Invalid s3 uri provided: `s3://bucket`"):
                ctx = ContextMock()
                event = {
                    "Records": [
                        {
                            "body": '{"output_type": "", "output_args": "", "event_payload": ""}',
                        }
                    ]
                }
                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: arn format too long"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Invalid arn format: "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:THIS:IS:INVALID",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:THIS:IS:INVALID"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty region"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Must be provided region in arn: " "arn:aws:secretsmanager::123456789:secret:plain_secret",
            ):
                ctx = ContextMock()
                # BEWARE region is empty at id
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager::123456789:secret:plain_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty secrets manager name"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Must be provided secrets manager name in arn: "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:",
            ):
                ctx = ContextMock()
                # BEWARE empty secrets manager name at id
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: cannot use both plain text and key/value pairs"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "You cannot have both plain text and json key for the same "
                "secret: arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username",
            ):
                ctx = ContextMock()
                # BEWARE using es_secrets plain text for elasticsearch_url and es_secrets:username for username
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secrets"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty secret key"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:: key must "
                "not be empty",
            ):
                ctx = ContextMock()
                # BEWARE empty key at elasticsearch_url
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: secret does not exist"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                r"An error occurred \(ResourceNotFoundException\) when calling "
                "the GetSecretValue operation: Secrets Manager can't find the specified secret.",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:DOES_NOT_EXIST"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty plain secret value"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:empty_secret: must "
                "not be empty",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:empty_secret"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: empty key/value secret value"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:empty: must "
                "not be empty",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:empty"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: plain text used as key/value"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:SHOULD_NOT_HAVE_A_KEY: "
                "expected to be keys/values pair",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:SHOULD_NOT_HAVE_A_KEY"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: key does not exist in secret manager"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:I_DO_NOT_EXIST: "
                "key not found",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:I_DO_NOT_EXIST"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: plain text secret not str"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte: "
                "expected to be a string",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("invalid secretsmanager: json TypeError risen"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "Expected string or C-contiguous bytes-like object while parsing "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("tags not list"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException, "`tags` must be provided as list for input mock_plain_text_sqs_arn"
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        tags: "tag1"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("each tag must be of type str"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                r"Each tag in `tags` must be provided as string for input "
                r"mock_plain_text_sqs_arn, given: \['tag1', 2, 'tag3'\]",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        tags:
                          - "tag1"
                          - 2
                          - "tag3"
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("expand_event_list_from_field not str"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "`expand_event_list_from_field` must be provided as string for input mock_plain_text_sqs_arn",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        expand_event_list_from_field: 0
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("root_fields_to_add_to_expanded_event not `all` when string"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "`root_fields_to_add_to_expanded_event` must be provided as `all` or a list of strings",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        root_fields_to_add_to_expanded_event: not_all
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("root_fields_to_add_to_expanded_event not `all` neither list of strings"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "`root_fields_to_add_to_expanded_event` must be provided as `all` or a list of strings",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        root_fields_to_add_to_expanded_event: 0
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore

        with self.subTest("json_content_type not valid"):
            os.environ["S3_CONFIG_FILE"] = "s3://s3_config_file_bucket/s3_config_file_object_key"
            with self.assertRaisesRegex(
                ConfigFileException,
                "`json_content_type` must be one of ndjson,single,disabled "
                "for input mock_plain_text_sqs_arn: whatever given",
            ):
                ctx = ContextMock()
                _s3_client_mock.config_content = b"""
                    inputs:
                      - type: "s3-sqs"
                        id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                        json_content_type: whatever
                        outputs:
                          - type: "elasticsearch"
                            args:
                              elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                              username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                              password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                              es_datastream_name: "logs-redis.log-default"
                """

                event = deepcopy(dummy_event)

                handler(event, ctx)  # type:ignore
