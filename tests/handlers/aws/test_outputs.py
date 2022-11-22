
import gzip
from share import json_dumper, json_parser
import boto3
import os, os.path

from main_aws import handler

from typing import Any

import mock
import pytest
from unittest import TestCase
from testcontainers.localstack import LocalStackContainer
from tests.testcontainers.logstash import LogstashContainer
from string import Template
# main test facilities
from tests.handlers.aws.test_handler import ContextMock
# test helpers
from tests.handlers.aws.test_handler import _s3_event_to_sqs_message, _event_from_sqs_message
import datetime
import botocore

def load_file_fixture(name:str) -> str:
    filepath = os.path.join(os.path.dirname(__file__), 'testdata', name)

    res = ""
    with open(filepath) as f:
        res = f.read()

    return res

def _upload_content_to_bucket(client, content, key:str, bucket_name:str, content_type:str, acl:str="public-read-write") -> None:
    client.create_bucket(Bucket=bucket_name, ACL=acl)
    client.put_object(Bucket=bucket_name, Key=key, Body=content, ContentType=content_type)

def _create_cloudwatch_logs_stream(client, group_name: str, stream_name: str) -> Any:
    client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)

    return client.describe_log_streams(logGroupName=group_name, logStreamNamePrefix=stream_name)["logStreams"][0]


def _create_cloudwatch_logs_group(client, group_name: str) -> Any:
    client.create_log_group(logGroupName=group_name)
    return client.describe_log_groups(logGroupNamePrefix=group_name)

def _event_to_cloudwatch_logs(client, group_name: str, stream_name: str, messages_body: list[str]) -> None:
    now = int(datetime.datetime.utcnow().strftime("%s")) * 1000
    client.put_log_events(
        logGroupName=group_name,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": now + (n * 1000), "message": message_body} for n, message_body in enumerate(messages_body)
        ],
    )

import random, string, base64
def _event_from_cloudwatch_logs(client, group_name: str, stream_name: str) -> tuple[dict[str, Any], list[str]]:
    collected_log_event_ids: list[str] = []
    collected_log_events: list[dict[str, Any]] = []

    events = client.get_log_events(logGroupName=group_name, logStreamName=stream_name)

    assert "events" in events
    for event in events["events"]:
        event_id = "".join(random.choices(string.digits, k=56))
        log_event = {
            "id": event_id,
            "timestamp": event["timestamp"],
            "message": event["message"],
        }

        collected_log_events.append(log_event)
        collected_log_event_ids.append(event_id)

    data_json = json_dumper(
        {
            "messageType": "DATA_MESSAGE",
            "owner": "000000000000",
            "logGroup": group_name,
            "logStream": stream_name,
            "subscriptionFilters": ["a-subscription-filter"],
            "logEvents": collected_log_events,
        }
    )

    data_gzip = gzip.compress(data_json.encode("UTF-8"))
    data_base64encoded = base64.b64encode(data_gzip)

    return {"awslogs": {"data": data_base64encoded}}, collected_log_event_ids


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
        lgc.with_env("CONFIG_STRING", '''\
            input {{
              http {{
                port => {logstash_http_port}
                codec => json_lines
              }}
            }}

            output {{ stdout {{}} }}
            '''.format(logstash_http_port=self.logstash_http_port))
        self.logstash = lgc.start()

        self.fixtures = {
                "cw_log_1": load_file_fixture('cloudwatch-log-1.json'),
                "cw_log_2": load_file_fixture('cloudwatch-log-2.json')
        }

        group_name = f"{type(self).__name__}-source-group"
        stream_name = f"{type(self).__name__}-source-stream"

        _create_cloudwatch_logs_group(self.logs_client, group_name=group_name)
        g = _create_cloudwatch_logs_stream(self.logs_client,
            group_name=group_name, stream_name=stream_name)
        cloudwatch_group_arn = g['arn']

        _event_to_cloudwatch_logs(
            self.logs_client,
            group_name=group_name,
            stream_name=stream_name,
            messages_body=[self.fixtures["cw_log_1"] + self.fixtures["cw_log_2"]],
        )

        self.group_name = group_name
        self.stream_name = stream_name

        config_content = load_file_fixture('config.yaml')
        self.config = Template(config_content).substitute(dict(
            CloudwatchLogStreamARN=cloudwatch_group_arn,
            LogstashURL=self.logstash.get_url()
        ))
        print(self.config)

        _upload_content_to_bucket(
            client=self.s3_client,
            content=self.config,
            content_type="text/plain",
            bucket_name=f"{type(self).__name__}-config-bucket",
            key="folder/config.yaml",
        )

        def _create_sqs_queue(client, name:string):
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

    def tearDown(self):
        self.localstack.stop()
        self.logstash.stop()

    def test_foo(self):
        event_cloudwatch_logs, event_ids_cloudwatch_logs = _event_from_cloudwatch_logs(
                self.logs_client,
            group_name=self.group_name, stream_name=self.stream_name
        )
        print(event_cloudwatch_logs, event_ids_cloudwatch_logs)

        ctx = ContextMock(1000*60*5)
        third_call = handler(event_cloudwatch_logs, ctx)
        print(third_call)
        # test new input => output to stdout

        # import time; time.sleep(9999)

