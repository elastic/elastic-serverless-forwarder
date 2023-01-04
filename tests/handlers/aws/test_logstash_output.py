# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from string import Template
from typing import Any
from unittest import TestCase

import boto3
import mock
import pytest
from testcontainers.localstack import LocalStackContainer  # type: ignore

from main_aws import handler
from share import json_parser
from tests.handlers.aws.test_handler import ContextMock
from tests.handlers.aws.utils import (
    _class_based_id,
    _load_file_fixture,
    _logs_create_cloudwatch_logs_group,
    _logs_create_cloudwatch_logs_stream,
    _logs_retrieve_event_from_cloudwatch_logs,
    _logs_upload_event_to_cloudwatch_logs,
    _s3_upload_content_to_bucket,
    _sqs_create_queue,
    _sqs_get_messages,
    _sqs_get_queue_arn,
    _sqs_patch_messages,
)
from tests.testcontainers.logstash import LogstashContainer

TIMEOUT_15m = 1000 * 60 * 15


def _prepare_config_file(klass: Any, conffixture: str, confdict: dict[str, Any], config_file_path: str) -> str:
    config_content = _load_file_fixture(conffixture)
    klass.config = Template(config_content).substitute(confdict)

    config_bucket_name = _class_based_id(klass, suffix="config-bucket").lower()
    _s3_upload_content_to_bucket(
        client=klass.s3_client,
        content=klass.config,
        content_type="text/plain",
        bucket_name=config_bucket_name,
        key=config_file_path,
    )

    return f"s3://{config_bucket_name}/{config_file_path}"


@pytest.mark.integration
class TestLambdaHandlerLogstashOutputSuccess(TestCase):
    def setUp(self) -> None:
        lst = LocalStackContainer(image="localstack/localstack:1.1.0")
        lst.with_env("EAGER_SERVICE_LOADING", "1")
        lst.with_services("s3", "logs", "sqs")
        self.localstack = lst.start()

        aws_default_region = "us-east-1"
        session = boto3.Session(region_name=aws_default_region)
        self.aws_session = session
        self.s3_client = session.client("s3", endpoint_url=self.localstack.get_url())
        self.logs_client = session.client("logs", endpoint_url=self.localstack.get_url())
        self.sqs_client = session.client("sqs", endpoint_url=self.localstack.get_url())

        self.logstash_http_port = 5043
        lgc = LogstashContainer(port=self.logstash_http_port)
        # NOTE: plain curly brackets must be escaped in this string (double them)
        logstash_config = f"""\
            input {{
              http {{
                port => {self.logstash_http_port}
                codec => json_lines
              }}
            }}

            output {{ stdout {{ codec => json_lines }} }}
            """
        lgc.with_env("CONFIG_STRING", logstash_config)
        self.logstash = lgc.start()

        self.fixtures = {
            "cw_log_1": _load_file_fixture("cloudwatch-log-1.json"),
            "cw_log_2": _load_file_fixture("cloudwatch-log-2.json"),
        }

        group_name = _class_based_id(self, suffix="source-group")
        stream_name = _class_based_id(self, suffix="source-stream")

        _logs_create_cloudwatch_logs_group(self.logs_client, group_name=group_name)
        cw_logstream = _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=group_name, stream_name=stream_name
        )
        self.cloudwatch_group_arn = cw_logstream["arn"]

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=group_name,
            stream_name=stream_name,
            messages_body=[self.fixtures["cw_log_1"] + self.fixtures["cw_log_2"]],
        )

        self.group_name = group_name
        self.stream_name = stream_name

        os.environ["AWS_DEFAULT_REGION"] = aws_default_region
        os.environ["SQS_CONTINUE_URL"] = _sqs_create_queue(self.sqs_client, _class_based_id(self, suffix="-continuing"))
        os.environ["SQS_REPLAY_URL"] = _sqs_create_queue(self.sqs_client, _class_based_id(self, suffix="-replay"))

        self.mocks = {
            "s3client": mock.patch("storage.S3Storage._s3_client", new=self.s3_client),
            "cloudwatchclient": mock.patch("handlers.aws.utils.get_cloudwatch_logs_client", lambda: self.logs_client),
            "sqsclient": mock.patch("handlers.aws.utils.get_sqs_client", lambda: self.sqs_client),
            "sqsclient2": mock.patch("handlers.aws.handler.get_sqs_client", lambda: self.sqs_client),
        }
        for k, m in self.mocks.items():
            m.start()

    def tearDown(self) -> None:
        self.localstack.stop()
        self.logstash.stop()

        for k, m in self.mocks.items():
            m.stop()

    def test_sent_messages(self) -> None:
        os.environ["S3_CONFIG_FILE"] = _prepare_config_file(
            self,
            "config.yaml",
            dict(CloudwatchLogStreamARN=self.cloudwatch_group_arn, LogstashURL=self.logstash.get_url()),
            "folder/config.yaml",
        )

        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )

        ctx = ContextMock(TIMEOUT_15m)
        handler(event_cloudwatch_logs, ctx)  # type: ignore

        msgs = self.logstash.get_messages()
        assert len(msgs) == 2

        assert msgs[0]["fields"]["message"] == self.fixtures["cw_log_1"].rstrip("\n")
        assert msgs[1]["fields"]["message"] == self.fixtures["cw_log_2"].rstrip("\n")

    def test_failure_sending_messages(self) -> None:
        os.environ["S3_CONFIG_FILE"] = _prepare_config_file(
            self,
            "config.yaml",
            # NOTE: using a bogus URL to mimic sending failure
            dict(CloudwatchLogStreamARN=self.cloudwatch_group_arn, LogstashURL="http://fake.url"),
            "folder/config2.yaml",
        )

        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )

        ctx = ContextMock(TIMEOUT_15m)
        handler(event_cloudwatch_logs, ctx)  # type: ignore

        msgs = self.logstash.get_messages()
        assert len(msgs) == 0

        messages = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"])
        assert len(messages) == 2

        event1 = json_parser(messages[0]["Body"])
        assert event1["event_payload"]["fields"]["message"] == self.fixtures["cw_log_1"].rstrip("\n")

        event2 = json_parser(messages[1]["Body"])
        assert event2["event_payload"]["fields"]["message"] == self.fixtures["cw_log_2"].rstrip("\n")

    def test_send_timeout(self) -> None:
        """
        This test verify that if a timeout happens events still to be sent are correctly
        sent to the continuing queue.
        """
        os.environ["S3_CONFIG_FILE"] = _prepare_config_file(
            self,
            "config.yaml",
            dict(CloudwatchLogStreamARN=self.cloudwatch_group_arn, LogstashURL=self.logstash.get_url()),
            "folder/config2.yaml",
        )

        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )

        # NOTE: 0 triggers sending to the continuing queue as it mimics imminent timeout
        ctx = ContextMock(0)

        # Run handler with inputs, check that resulting action is continue and messages are
        # flushed to output and sent to continuing queue
        result = handler(event_cloudwatch_logs, ctx)  # type: ignore
        assert result == "continuing"
        msgs = self.logstash.get_messages()
        assert len(msgs) == 1
        assert msgs[0]["fields"]["message"] == self.fixtures["cw_log_1"].rstrip("\n")
        messages = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"])
        assert messages[0]["Body"] == self.fixtures["cw_log_1"] + self.fixtures["cw_log_2"]

    def test_after_timeout_continuation(self) -> None:
        """
        This test verify that the offset management logic after a timeout do not replay
        already sent events.
        """
        os.environ["S3_CONFIG_FILE"] = _prepare_config_file(
            self,
            "config.yaml",
            dict(CloudwatchLogStreamARN=self.cloudwatch_group_arn, LogstashURL=self.logstash.get_url()),
            "folder/config2.yaml",
        )

        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )

        # NOTE: 0 triggers sending to the continuing queue as it mimics imminent timeout
        ctx = ContextMock(0)

        # Run handler, check sends data to continuing queue. Check on this behaviour is
        # done by test_send_timeout.
        result = handler(event_cloudwatch_logs, ctx)  # type: ignore
        assert result == "continuing"

        # Run handler again, check that execution completes and the second message is sent
        # to output.
        messages = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"])
        queue_arn = _sqs_get_queue_arn(self.sqs_client, os.environ["SQS_CONTINUE_URL"])
        continued_messages = _sqs_patch_messages(messages, queue_arn)
        result = handler(continued_messages, ContextMock(TIMEOUT_15m))  # type: ignore
        assert result == "completed"
        msgs = self.logstash.get_messages()
        assert len(msgs) == 2
        # NOTE: state is retained, so all messages are present in the returned data
        assert msgs[1]["fields"]["message"] == self.fixtures["cw_log_2"].rstrip("\n")

    def test_message_content(self) -> None:

        os.environ["S3_CONFIG_FILE"] = _prepare_config_file(
            self,
            "config.yaml",
            dict(CloudwatchLogStreamARN=self.cloudwatch_group_arn, LogstashURL=self.logstash.get_url()),
            "folder/config2.yaml",
        )

        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )

        ctx = ContextMock(TIMEOUT_15m)

        # Run handler with inputs, check that resulting action is continue and messages are
        # flushed to output and sent to continuing queue
        result = handler(event_cloudwatch_logs, ctx)  # type: ignore
        assert result == "completed"
        msgs = self.logstash.get_messages()
        assert len(msgs) == 2

        group_name = _class_based_id(self, suffix="source-group")
        stream_name = _class_based_id(self, suffix="source-stream")
        assert msgs[0]["fields"]["aws"]["cloudwatch"]["log_group"] == group_name
        assert msgs[0]["fields"]["aws"]["cloudwatch"]["log_stream"] == stream_name
        assert msgs[0]["fields"]["cloud"]["provider"] == "aws"
        assert msgs[0]["fields"]["cloud"]["region"] == os.environ["AWS_DEFAULT_REGION"]
        assert msgs[0]["fields"]["cloud"]["account"]["id"] == "000000000000"
        assert msgs[0]["fields"]["log"]["offset"] == 0
        assert msgs[0]["fields"]["log"]["file"]["path"] == f"{group_name}/{stream_name}"
        assert msgs[0]["fields"]["message"] == self.fixtures["cw_log_1"].rstrip("\n")

        assert msgs[1]["fields"]["aws"]["cloudwatch"]["log_group"] == group_name
        assert msgs[1]["fields"]["aws"]["cloudwatch"]["log_stream"] == stream_name
        assert msgs[1]["fields"]["cloud"]["provider"] == "aws"
        assert msgs[1]["fields"]["cloud"]["region"] == os.environ["AWS_DEFAULT_REGION"]
        assert msgs[1]["fields"]["cloud"]["account"]["id"] == "000000000000"
        assert msgs[1]["fields"]["log"]["offset"] == 94
        assert msgs[1]["fields"]["log"]["file"]["path"] == f"{group_name}/{stream_name}"
        assert msgs[1]["fields"]["message"] == self.fixtures["cw_log_2"].rstrip("\n")
