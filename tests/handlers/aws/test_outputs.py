import os
from string import Template
from typing import Any
from unittest import TestCase

import boto3
import mock
import pytest
from botocore.client import BaseClient as BotoBaseClient
from testcontainers.localstack import LocalStackContainer  # type: ignore

from main_aws import handler
from tests.handlers.aws.test_handler import ContextMock
from tests.handlers.aws.utils import (
    _class_based_id,
    _load_file_fixture,
    _logs_create_cloudwatch_logs_group,
    _logs_create_cloudwatch_logs_stream,
    _logs_retrieve_event_from_cloudwatch_logs,
    _logs_upload_event_to_cloudwatch_logs,
    _s3_upload_content_to_bucket,
)
from tests.testcontainers.logstash import LogstashContainer  # type: ignore


@pytest.mark.integration
class TestLambdaHandlerLogstashOutputSuccess(TestCase):
    def setUp(self) -> None:
        LocalStackContainer.IMAGE = "localstack/localstack:1.1.0"
        lst = LocalStackContainer()
        lst.with_env("EAGER_SERVICE_LOADING", 1)
        lst.with_services("s3", "logs", "sqs")
        self.localstack = lst.start()

        session = boto3.Session(region_name="eu-west-1")
        self.s3_client = session.client("s3", endpoint_url=self.localstack.get_url())
        self.logs_client = session.client("logs", endpoint_url=self.localstack.get_url())
        self.sqs_client = session.client("sqs", endpoint_url=self.localstack.get_url())

        self.logstash_http_port = 5043
        lgc = LogstashContainer(port=self.logstash_http_port)
        logstash_config = """\
            input {{
              http {{
                port => {logstash_http_port}
                codec => json_lines
              }}
            }}

            output {{ stdout {{ codec => json_lines }} }}
            """.format(
            logstash_http_port=self.logstash_http_port
        )
        print(logstash_config)
        lgc.with_env("CONFIG_STRING", logstash_config)
        self.logstash = lgc.start()

        self.fixtures = {
            "cw_log_1": _load_file_fixture("cloudwatch-log-1.json"),
            "cw_log_2": _load_file_fixture("cloudwatch-log-2.json"),
        }

        group_name = _class_based_id(self, suffix="source-group")
        stream_name = _class_based_id(self, suffix="source-stream")

        _logs_create_cloudwatch_logs_group(self.logs_client, group_name=group_name)
        g = _logs_create_cloudwatch_logs_stream(self.logs_client, group_name=group_name, stream_name=stream_name)
        cloudwatch_group_arn = g["arn"]

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=group_name,
            stream_name=stream_name,
            messages_body=[self.fixtures["cw_log_1"] + self.fixtures["cw_log_2"]],
        )

        self.group_name = group_name
        self.stream_name = stream_name

        config_content = _load_file_fixture("config.yaml")
        self.config = Template(config_content).substitute(
            dict(CloudwatchLogStreamARN=cloudwatch_group_arn, LogstashURL=self.logstash.get_url())
        )
        print(self.config)

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=self.config,
            content_type="text/plain",
            bucket_name=_class_based_id(self, suffix="config-bucket").lower(),
            key="folder/config.yaml",
        )

        def _create_sqs_queue(client: BotoBaseClient, name: str) -> Any:
            q = client.create_queue(QueueName=name)
            print(q["QueueUrl"])
            return q["QueueUrl"]

        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["S3_CONFIG_FILE"] = f"s3://{type(self).__name__}-config-bucket/folder/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = _create_sqs_queue(self.sqs_client, f"{type(self).__name__}-continuing")
        os.environ["SQS_REPLAY_URL"] = _create_sqs_queue(self.sqs_client, f"{type(self).__name__}-replay")

        mock.patch("storage.S3Storage._s3_client", new=self.s3_client).start()
        mock.patch("handlers.aws.utils.get_cloudwatch_logs_client", lambda: self.logs_client).start()
        mock.patch("handlers.aws.utils.get_sqs_client", lambda: self.sqs_client).start()

    def tearDown(self) -> None:
        self.localstack.stop()
        self.logstash.stop()

    def test_foo(self) -> None:
        event_cloudwatch_logs, event_ids_cloudwatch_logs = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, group_name=self.group_name, stream_name=self.stream_name
        )
        print(event_cloudwatch_logs, event_ids_cloudwatch_logs)

        ctx = ContextMock(1000 * 60 * 5)
        third_call = handler(event_cloudwatch_logs, ctx)  # type: ignore
        print(third_call)
        # test new input => output to stdout

        msgs = self.logstash.get_messages()
        assert len(msgs) == 2
