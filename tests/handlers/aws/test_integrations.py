# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import gzip
import os
import time
from typing import Any, Optional
from unittest import TestCase

import boto3
import mock
import pytest
from botocore.client import BaseClient as BotoBaseClient
from testcontainers.localstack import LocalStackContainer

from handlers.aws.exceptions import ReplayHandlerException
from main_aws import handler
from share import get_hex_prefix, json_dumper, json_parser
from tests.testcontainers.es import ElasticsearchContainer
from tests.testcontainers.logstash import LogstashContainer

from .utils import (
    _AWS_REGION,
    _S3_NOTIFICATION_EVENT_TIME,
    ContextMock,
    _create_secrets,
    _kinesis_create_stream,
    _kinesis_put_records,
    _kinesis_retrieve_event_from_kinesis_stream,
    _load_file_fixture,
    _logs_create_cloudwatch_logs_group,
    _logs_create_cloudwatch_logs_stream,
    _logs_retrieve_event_from_cloudwatch_logs,
    _logs_upload_event_to_cloudwatch_logs,
    _REMAINING_TIME_FORCE_CONTINUE_0ms,
    _s3_upload_content_to_bucket,
    _sqs_create_queue,
    _sqs_get_messages,
    _sqs_send_messages,
    _sqs_send_s3_notifications,
    _time_based_id,
)

_OVER_COMPLETION_GRACE_PERIOD_2m = 1 + (1000 * 60 * 2)


@pytest.mark.integration
class TestLambdaHandlerIntegration(TestCase):
    elasticsearch: Optional[ElasticsearchContainer] = None
    logstash: Optional[LogstashContainer] = None
    localstack: Optional[LocalStackContainer] = None

    aws_session: Optional[boto3.Session] = None
    s3_client: Optional[BotoBaseClient] = None
    logs_client: Optional[BotoBaseClient] = None
    sqs_client: Optional[BotoBaseClient] = None
    kinesis_client: Optional[BotoBaseClient] = None
    sm_client: Optional[BotoBaseClient] = None
    ec2_client: Optional[BotoBaseClient] = None

    secret_arn: Optional[Any] = None

    mocks: dict[str, Any] = {}

    @classmethod
    def setUpClass(cls) -> None:
        esc = ElasticsearchContainer()
        cls.elasticsearch = esc.start()

        lgc = LogstashContainer(es_container=esc)
        cls.logstash = lgc.start()

        lsc = LocalStackContainer(image="localstack/localstack:3.0.1")
        lsc.with_env("EAGER_SERVICE_LOADING", "1")
        lsc.with_env("SQS_DISABLE_CLOUDWATCH_METRICS", "1")
        lsc.with_services("ec2", "kinesis", "logs", "s3", "sqs", "secretsmanager")

        cls.localstack = lsc.start()

        session = boto3.Session(region_name=_AWS_REGION)
        cls.aws_session = session
        cls.s3_client = session.client("s3", endpoint_url=cls.localstack.get_url())
        cls.logs_client = session.client("logs", endpoint_url=cls.localstack.get_url())
        cls.sqs_client = session.client("sqs", endpoint_url=cls.localstack.get_url())
        cls.kinesis_client = session.client("kinesis", endpoint_url=cls.localstack.get_url())
        cls.sm_client = session.client("secretsmanager", endpoint_url=cls.localstack.get_url())
        cls.ec2_client = session.client("ec2", endpoint_url=cls.localstack.get_url())

        cls.secret_arn = _create_secrets(
            cls.sm_client,
            "es_secrets",
            {"username": cls.elasticsearch.elastic_user, "password": cls.elasticsearch.elastic_password},
        )

        cls.mocks = {
            "storage.S3Storage._s3_client": mock.patch("storage.S3Storage._s3_client", new=cls.s3_client),
            "share.secretsmanager._get_aws_sm_client": mock.patch(
                "share.secretsmanager._get_aws_sm_client", lambda region_name: cls.sm_client
            ),
            "handlers.aws.utils.get_sqs_client": mock.patch(
                "handlers.aws.utils.get_sqs_client", lambda: cls.sqs_client
            ),
            "handlers.aws.utils.get_ec2_client": mock.patch(
                "handlers.aws.utils.get_ec2_client", lambda: cls.ec2_client
            ),
            "handlers.aws.handler.get_sqs_client": mock.patch(
                "handlers.aws.handler.get_sqs_client", lambda: cls.sqs_client
            ),
        }

        for k, m in cls.mocks.items():
            m.start()

    @classmethod
    def tearDownClass(cls) -> None:
        assert cls.elasticsearch is not None
        assert cls.logstash is not None
        assert cls.localstack is not None

        cls.elasticsearch.stop()
        cls.logstash.stop()
        cls.localstack.stop()

        for k, m in cls.mocks.items():
            m.stop()

    def setUp(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        os.environ["S3_CONFIG_FILE"] = ""

        sqs_continue_queue = _sqs_create_queue(self.sqs_client, _time_based_id(suffix="continuing"))
        sqs_replay_queue = _sqs_create_queue(self.sqs_client, _time_based_id(suffix="replay"))
        os.environ["SQS_CONTINUE_URL"] = sqs_continue_queue["QueueUrl"]
        os.environ["SQS_REPLAY_URL"] = sqs_replay_queue["QueueUrl"]

        self.sqs_continue_queue_arn = sqs_continue_queue["QueueArn"]
        self.sqs_replay_queue_arn = sqs_replay_queue["QueueArn"]

        self.default_tags: str = """
                  - "tag1"
                  - "tag2"
                  - "tag3"
        """

        self.default_outputs: str = f"""
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

    def tearDown(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)

        self.logstash.reset()
        self.elasticsearch.reset()

        os.environ["S3_CONFIG_FILE"] = ""
        os.environ["SQS_CONTINUE_URL"] = ""
        os.environ["SQS_REPLAY_URL"] = ""

    def test_ls_es_output(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        config_yaml: str = f"""
            inputs:
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        cloudtrail_filename_digest = (
            "AWSLogs/aws-account-id/CloudTrail-Digest/region/yyyy/mm/dd/"
            "aws-account-id_CloudTrail-Digest_region_end-time_random-string.log.gz"
        )
        cloudtrail_filename_non_digest = (
            "AWSLogs/aws-account-id/CloudTrail/region/yyyy/mm/dd/"
            "aws-account-id_CloudTrail_region_end-time_random-string.log.gz"
        )

        s3_bucket_name = _time_based_id(suffix="test-bucket")

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[0].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=cloudtrail_filename_digest,
        )

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[1].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=cloudtrail_filename_non_digest,
        )

        _sqs_send_s3_notifications(
            self.sqs_client,
            s3_sqs_queue_url,
            s3_bucket_name,
            [cloudtrail_filename_digest, cloudtrail_filename_non_digest],
        )

        event, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-aws.cloudtrail-default")
        assert self.elasticsearch.count(index="logs-aws.cloudtrail-default")["count"] == 2

        res = self.elasticsearch.search(index="logs-aws.cloudtrail-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

        assert res["hits"]["hits"][0]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][0]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][0]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_digest}"
        )
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_digest
        assert res["hits"]["hits"][0]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][0]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][1]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][1]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][1]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_non_digest}"
        )
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_non_digest
        assert res["hits"]["hits"][1]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][1]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2
        res["hits"]["hits"][0]["_source"]["tags"].remove("aws-cloudtrail")
        res["hits"]["hits"][1]["_source"]["tags"].remove("aws-cloudtrail")

        assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]

        assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]

        self.elasticsearch.refresh(index="logs-stash.elasticsearch-output")
        assert self.elasticsearch.count(index="logs-stash.elasticsearch-output")["count"] == 2

        res = self.elasticsearch.search(index="logs-stash.elasticsearch-output", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

        assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]

        assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]

    def test_continuing(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress("".join(fixtures).encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=["".join(fixtures)],
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())
        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))
        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, ["".join(fixtures)])

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        events_cloudwatch_logs, event_ids_cloudwatch_logs, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock()
        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "continuing"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 1

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}

        assert res["hits"]["hits"][0]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][0]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][0]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["object"]["key"] == first_filename
        assert res["hits"]["hits"][0]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][0]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1
        res["hits"]["hits"][0]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]

        second_call = handler(events_sqs, ctx)  # type:ignore

        assert second_call == "continuing"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 2

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

        assert res["hits"]["hits"][1]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][1]["_source"]["log"]["offset"] == 0
        assert res["hits"]["hits"][1]["_source"]["log"]["file"]["path"] == sqs_queue_url_path
        assert res["hits"]["hits"][1]["_source"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert res["hits"]["hits"][1]["_source"]["aws"]["sqs"]["message_id"] == message_id
        assert res["hits"]["hits"][1]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][1]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2
        res["hits"]["hits"][1]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]

        third_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert third_call == "continuing"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 3

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

        assert res["hits"]["hits"][2]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][2]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][2]["_source"]["log"]["file"]["path"]
            == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        )
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert res["hits"]["hits"][2]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][2]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][2]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][2]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3
        res["hits"]["hits"][2]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][2]["_source"]["aws"] == logstash_message[2]["aws"]
        assert res["hits"]["hits"][2]["_source"]["cloud"] == logstash_message[2]["cloud"]
        assert res["hits"]["hits"][2]["_source"]["log"] == logstash_message[2]["log"]
        assert res["hits"]["hits"][2]["_source"]["message"] == logstash_message[2]["message"]
        assert res["hits"]["hits"][2]["_source"]["tags"] == logstash_message[2]["tags"]

        fourth_call = handler(events_kinesis, ctx)  # type:ignore

        assert fourth_call == "continuing"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 4

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 4, "relation": "eq"}

        assert res["hits"]["hits"][3]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][3]["_source"]["log"]["offset"] == 0
        assert res["hits"]["hits"][3]["_source"]["log"]["file"]["path"] == kinesis_stream_arn
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["type"] == "stream"
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert res["hits"]["hits"][3]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][3]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][3]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][3]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=4)
        assert len(logstash_message) == 4
        res["hits"]["hits"][3]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][3]["_source"]["aws"] == logstash_message[3]["aws"]
        assert res["hits"]["hits"][3]["_source"]["cloud"] == logstash_message[3]["cloud"]
        assert res["hits"]["hits"][3]["_source"]["log"] == logstash_message[3]["log"]
        assert res["hits"]["hits"][3]["_source"]["message"] == logstash_message[3]["message"]
        assert res["hits"]["hits"][3]["_source"]["tags"] == logstash_message[3]["tags"]

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )

        fifth_call = handler(continued_events, ctx)  # type:ignore

        assert fifth_call == "continuing"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 5

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 5, "relation": "eq"}

        assert res["hits"]["hits"][4]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][4]["_source"]["log"]["offset"] == 94
        assert (
            res["hits"]["hits"][4]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["object"]["key"] == first_filename
        assert res["hits"]["hits"][4]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][4]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][4]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][4]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=5)
        assert len(logstash_message) == 5
        res["hits"]["hits"][4]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][4]["_source"]["aws"] == logstash_message[4]["aws"]
        assert res["hits"]["hits"][4]["_source"]["cloud"] == logstash_message[4]["cloud"]
        assert res["hits"]["hits"][4]["_source"]["log"] == logstash_message[4]["log"]
        assert res["hits"]["hits"][4]["_source"]["message"] == logstash_message[4]["message"]
        assert res["hits"]["hits"][4]["_source"]["tags"] == logstash_message[4]["tags"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        sixth_call = handler(continued_events, ctx)  # type:ignore

        assert sixth_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 8

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 8, "relation": "eq"}

        assert res["hits"]["hits"][5]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][5]["_source"]["log"]["offset"] == 94
        assert res["hits"]["hits"][5]["_source"]["log"]["file"]["path"] == sqs_queue_url_path
        assert res["hits"]["hits"][5]["_source"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert res["hits"]["hits"][5]["_source"]["aws"]["sqs"]["message_id"] == message_id
        assert res["hits"]["hits"][5]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][5]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][5]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][5]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][6]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][6]["_source"]["log"]["offset"] == 94
        assert (
            res["hits"]["hits"][6]["_source"]["log"]["file"]["path"]
            == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        )
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert res["hits"]["hits"][6]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][6]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][6]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][6]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][7]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][7]["_source"]["log"]["offset"] == 94
        assert res["hits"]["hits"][7]["_source"]["log"]["file"]["path"] == kinesis_stream_arn
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["type"] == "stream"
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert res["hits"]["hits"][7]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][7]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][7]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][7]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=8)
        assert len(logstash_message) == 8
        res["hits"]["hits"][5]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][6]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][7]["_source"]["tags"].remove("generic")

        assert res["hits"]["hits"][5]["_source"]["aws"] == logstash_message[5]["aws"]
        assert res["hits"]["hits"][5]["_source"]["cloud"] == logstash_message[5]["cloud"]
        assert res["hits"]["hits"][5]["_source"]["log"] == logstash_message[5]["log"]
        assert res["hits"]["hits"][5]["_source"]["message"] == logstash_message[5]["message"]
        assert res["hits"]["hits"][5]["_source"]["tags"] == logstash_message[5]["tags"]

        assert res["hits"]["hits"][6]["_source"]["aws"] == logstash_message[6]["aws"]
        assert res["hits"]["hits"][6]["_source"]["cloud"] == logstash_message[6]["cloud"]
        assert res["hits"]["hits"][6]["_source"]["log"] == logstash_message[6]["log"]
        assert res["hits"]["hits"][6]["_source"]["message"] == logstash_message[6]["message"]
        assert res["hits"]["hits"][6]["_source"]["tags"] == logstash_message[6]["tags"]

        assert res["hits"]["hits"][7]["_source"]["aws"] == logstash_message[7]["aws"]
        assert res["hits"]["hits"][7]["_source"]["cloud"] == logstash_message[7]["cloud"]
        assert res["hits"]["hits"][7]["_source"]["log"] == logstash_message[7]["log"]
        assert res["hits"]["hits"][7]["_source"]["message"] == logstash_message[7]["message"]
        assert res["hits"]["hits"][7]["_source"]["tags"] == logstash_message[7]["tags"]

    def test_continuing_no_timeout_input_from_originalEventSourceARN_message_attribute(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, fixtures[0])
        _sqs_send_messages(self.sqs_client, sqs_queue_url, fixtures[1])
        _sqs_send_messages(self.sqs_client, sqs_queue_url, fixtures[2])

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        first_message_id = events_sqs["Records"][0]["messageId"]
        second_message_id = events_sqs["Records"][1]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == first_message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )

        continued_events["Records"][2]["messageAttributes"]["originalEventSourceARN"][
            "stringValue"
        ] += "-not-configured-arn"
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

        assert logstash_message[1]["message"] == fixtures[1].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 0
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == second_message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )

        third_call = handler(continued_events, ctx)  # type:ignore

        assert third_call == "completed"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

    def test_replay(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress("".join(fixtures).encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=["".join(fixtures)],
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())
        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))
        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, ["".join(fixtures)])

        # the way to let logstash fail is to give wrong credentials
        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "wrong_username"
                      password: "wrong_username"
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "wrong_username"
                      password: "wrong_username"
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "wrong_username"
                      password: "wrong_username"
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "wrong_username"
                      password: "wrong_username"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        bucket_arn: str = f"arn:aws:s3:::{s3_bucket_name}"
        event_time = int(
            datetime.datetime.strptime(_S3_NOTIFICATION_EVENT_TIME, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000
        )

        hash_first = get_hex_prefix(f"{bucket_arn}-{first_filename}")
        prefix_s3_first = f"{event_time}-{hash_first}"

        events_sqs, events_sent_timestamps_sqs = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]
        hash_sqs = get_hex_prefix(f"{sqs_queue_name}-{message_id}")
        prefix_sqs: str = f"{events_sent_timestamps_sqs[0]}-{hash_sqs}"

        (
            events_cloudwatch_logs,
            event_ids_cloudwatch_logs,
            event_timestamps_cloudwatch_logs,
        ) = _logs_retrieve_event_from_cloudwatch_logs(self.logs_client, cloudwatch_group_name, cloudwatch_stream_name)

        hash_cw_logs = get_hex_prefix(
            f"{cloudwatch_group_name}-{cloudwatch_stream_name}-{event_ids_cloudwatch_logs[0]}"
        )
        prefix_cloudwatch_logs = f"{event_timestamps_cloudwatch_logs[0]}-{hash_cw_logs}"

        events_kinesis, event_timestamps_kinesis_records = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )
        sequence_number = events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        hash_kinesis_record = get_hex_prefix(f"stream-{kinesis_stream_name}-PartitionKey-{sequence_number}")
        prefix_kinesis = f"{int(float(event_timestamps_kinesis_records[0]) * 1000)}-{hash_kinesis_record}"

        # Create pipeline to reject documents
        processors = {
            "processors": [
                {
                    "fail": {
                        "message": "test_replay_fail_pipeline_s3",
                        "if": f'ctx["_id"] == "{prefix_s3_first}-000000000000"',
                    }
                },
                {
                    "fail": {
                        "message": "test_replay_fail_pipeline_sqs",
                        "if": f'ctx["_id"] == "{prefix_sqs}-000000000000"',
                    }
                },
                {
                    "fail": {
                        "message": "test_replay_fail_pipeline_cloudwatch",
                        "if": f'ctx["_id"] == "{prefix_cloudwatch_logs}-000000000000"',
                    }
                },
                {
                    "fail": {
                        "message": "test_replay_fail_pipeline_kinesis",
                        "if": f'ctx["_id"] == "{prefix_kinesis}-000000000000"',
                    }
                },
            ]
        }

        self.elasticsearch.put_pipeline(id="test_replay_fail_pipeline", body=processors)

        self.elasticsearch.create_data_stream(name="logs-generic-default")
        self.elasticsearch.put_settings(
            index="logs-generic-default", body={"index.default_pipeline": "test_replay_fail_pipeline"}
        )

        self.elasticsearch.refresh(index="logs-generic-default")

        res = self.elasticsearch.search(index="logs-generic-default")
        assert res["hits"]["total"] == {"value": 0, "relation": "eq"}

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(
            index="logs-generic-default",
            sort="_seq_no",
        )

        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}

        assert res["hits"]["hits"][0]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][0]["_source"]["log"]["offset"] == 94
        assert (
            res["hits"]["hits"][0]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["object"]["key"] == first_filename
        assert res["hits"]["hits"][0]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][0]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        second_call = handler(events_sqs, ctx)  # type:ignore

        assert second_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(
            index="logs-generic-default",
            sort="_seq_no",
        )

        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

        assert res["hits"]["hits"][1]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][1]["_source"]["log"]["offset"] == 94
        assert res["hits"]["hits"][1]["_source"]["log"]["file"]["path"] == sqs_queue_url_path
        assert res["hits"]["hits"][1]["_source"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert res["hits"]["hits"][1]["_source"]["aws"]["sqs"]["message_id"] == message_id
        assert res["hits"]["hits"][1]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][1]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        third_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert third_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(
            index="logs-generic-default",
            sort="_seq_no",
        )

        assert res["hits"]["total"] == {"value": 3, "relation": "eq"}

        assert res["hits"]["hits"][2]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][2]["_source"]["log"]["offset"] == 94
        assert (
            res["hits"]["hits"][2]["_source"]["log"]["file"]["path"]
            == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        )
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert res["hits"]["hits"][2]["_source"]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert res["hits"]["hits"][2]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][2]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][2]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][2]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        fourth_call = handler(events_kinesis, ctx)  # type:ignore

        assert fourth_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(
            index="logs-generic-default",
            sort="_seq_no",
        )

        assert res["hits"]["total"] == {"value": 4, "relation": "eq"}

        assert res["hits"]["hits"][3]["_source"]["message"] == fixtures[1].rstrip("\n")
        assert res["hits"]["hits"][3]["_source"]["log"]["offset"] == 94
        assert res["hits"]["hits"][3]["_source"]["log"]["file"]["path"] == kinesis_stream_arn
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["type"] == "stream"
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            res["hits"]["hits"][3]["_source"]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert res["hits"]["hits"][3]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][3]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][3]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][3]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        with self.assertRaises(ReplayHandlerException):
            handler(replayed_events, ctx)  # type:ignore

        self.elasticsearch.refresh(index="logs-generic-default")

        # Remove pipeline processors
        processors = {"processors": []}

        self.elasticsearch.put_pipeline(id="test_replay_fail_pipeline", body=processors)
        self.elasticsearch.refresh(index="logs-generic-default")

        # let's update the config file so that logstash won't fail anymore
        config_yaml = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
        """

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
            create_bucket=False,
        )

        ctx = ContextMock(remaining_time_in_millis=_REMAINING_TIME_FORCE_CONTINUE_0ms)

        # implicit wait for the message to be back on the queue
        time.sleep(35)
        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        fifth_call = handler(replayed_events, ctx)  # type:ignore

        assert fifth_call == "replayed"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 5

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")

        assert res["hits"]["total"] == {"value": 5, "relation": "eq"}

        assert res["hits"]["hits"][4]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][4]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][4]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][4]["_source"]["aws"]["s3"]["object"]["key"] == first_filename
        assert res["hits"]["hits"][4]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][4]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][4]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][4]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        # implicit wait for the message to be back on the queue
        time.sleep(35)
        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        sixth_call = handler(replayed_events, ctx)  # type:ignore

        assert sixth_call == "replayed"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 5

        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 5, "relation": "eq"}

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1
        # positions on res["hits"]["hits"] are skewed compared to logstash_message
        # in elasticsearch we inserted the second event of each input before the first one
        res["hits"]["hits"][4]["_source"]["tags"].remove("generic")
        assert res["hits"]["hits"][4]["_source"]["aws"] == logstash_message[0]["aws"]
        assert res["hits"]["hits"][4]["_source"]["cloud"] == logstash_message[0]["cloud"]
        assert res["hits"]["hits"][4]["_source"]["log"] == logstash_message[0]["log"]
        assert res["hits"]["hits"][4]["_source"]["message"] == logstash_message[0]["message"]
        assert res["hits"]["hits"][4]["_source"]["tags"] == logstash_message[0]["tags"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        # implicit wait for the message to be back on the queue
        time.sleep(35)
        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        seventh_call = handler(replayed_events, ctx)  # type:ignore

        assert seventh_call == "replayed"

        self.elasticsearch.refresh(index="logs-generic-default")
        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 8

        self.elasticsearch.refresh(index="logs-generic-default")
        res = self.elasticsearch.search(index="logs-generic-default", sort="_seq_no")

        assert res["hits"]["total"] == {"value": 8, "relation": "eq"}

        assert res["hits"]["hits"][5]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][5]["_source"]["log"]["offset"] == 0
        assert res["hits"]["hits"][5]["_source"]["log"]["file"]["path"] == sqs_queue_url_path
        assert res["hits"]["hits"][5]["_source"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert res["hits"]["hits"][5]["_source"]["aws"]["sqs"]["message_id"] == message_id
        assert res["hits"]["hits"][5]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][5]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][5]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][5]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][6]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][6]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][6]["_source"]["log"]["file"]["path"]
            == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        )
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert res["hits"]["hits"][6]["_source"]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert res["hits"]["hits"][6]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][6]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][6]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][6]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][7]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][7]["_source"]["log"]["offset"] == 0
        assert res["hits"]["hits"][7]["_source"]["log"]["file"]["path"] == kinesis_stream_arn
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["type"] == "stream"
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            res["hits"]["hits"][7]["_source"]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert res["hits"]["hits"][7]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][7]["_source"]["cloud"]["region"] == "us-east-1"
        assert res["hits"]["hits"][7]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][7]["_source"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=8)
        assert len(logstash_message) == 8
        res["hits"]["hits"][0]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][1]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][2]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][3]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][5]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][6]["_source"]["tags"].remove("generic")
        res["hits"]["hits"][7]["_source"]["tags"].remove("generic")

        # positions on res["hits"]["hits"] are skewed compared to logstash_message
        # in elasticsearch we inserted the second event of each input before the first one
        assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[1]["aws"]
        assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[1]["cloud"]
        assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[1]["log"]
        assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[1]["message"]
        assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[1]["tags"]

        assert res["hits"]["hits"][5]["_source"]["aws"] == logstash_message[2]["aws"]
        assert res["hits"]["hits"][5]["_source"]["cloud"] == logstash_message[2]["cloud"]
        assert res["hits"]["hits"][5]["_source"]["log"] == logstash_message[2]["log"]
        assert res["hits"]["hits"][5]["_source"]["message"] == logstash_message[2]["message"]
        assert res["hits"]["hits"][5]["_source"]["tags"] == logstash_message[2]["tags"]

        assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[3]["aws"]
        assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[3]["cloud"]
        assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[3]["log"]
        assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[3]["message"]
        assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[3]["tags"]

        assert res["hits"]["hits"][6]["_source"]["aws"] == logstash_message[4]["aws"]
        assert res["hits"]["hits"][6]["_source"]["cloud"] == logstash_message[4]["cloud"]
        assert res["hits"]["hits"][6]["_source"]["log"] == logstash_message[4]["log"]
        assert res["hits"]["hits"][6]["_source"]["message"] == logstash_message[4]["message"]
        assert res["hits"]["hits"][6]["_source"]["tags"] == logstash_message[4]["tags"]

        assert res["hits"]["hits"][2]["_source"]["aws"] == logstash_message[5]["aws"]
        assert res["hits"]["hits"][2]["_source"]["cloud"] == logstash_message[5]["cloud"]
        assert res["hits"]["hits"][2]["_source"]["log"] == logstash_message[5]["log"]
        assert res["hits"]["hits"][2]["_source"]["message"] == logstash_message[5]["message"]
        assert res["hits"]["hits"][2]["_source"]["tags"] == logstash_message[5]["tags"]

        assert res["hits"]["hits"][7]["_source"]["aws"] == logstash_message[6]["aws"]
        assert res["hits"]["hits"][7]["_source"]["cloud"] == logstash_message[6]["cloud"]
        assert res["hits"]["hits"][7]["_source"]["log"] == logstash_message[6]["log"]
        assert res["hits"]["hits"][7]["_source"]["message"] == logstash_message[6]["message"]
        assert res["hits"]["hits"][7]["_source"]["tags"] == logstash_message[6]["tags"]

        assert res["hits"]["hits"][3]["_source"]["aws"] == logstash_message[7]["aws"]
        assert res["hits"]["hits"][3]["_source"]["cloud"] == logstash_message[7]["cloud"]
        assert res["hits"]["hits"][3]["_source"]["log"] == logstash_message[7]["log"]
        assert res["hits"]["hits"][3]["_source"]["message"] == logstash_message[7]["message"]
        assert res["hits"]["hits"][3]["_source"]["tags"] == logstash_message[7]["tags"]

    def test_empty(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [" \n"]  # once stripped it is an empty event

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress("".join(fixtures).encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=["".join(fixtures)],
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())
        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))
        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, ["".join(fixtures)])

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                outputs: {self.default_outputs}
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                outputs: {self.default_outputs}
              - type: sqs
                id: "{sqs_queue_arn}"
                outputs: {self.default_outputs}
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                outputs: {self.default_outputs}
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        events_cloudwatch_logs, _, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        second_call = handler(events_sqs, ctx)  # type:ignore

        assert second_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        third_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert third_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        fourth_call = handler(events_kinesis, ctx)  # type:ignore

        assert fourth_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

    def test_filtered(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = ["excluded"]

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress("".join(fixtures).encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=["".join(fixtures)],
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())
        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))
        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, ["".join(fixtures)])

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                exclude:
                  - "excluded"
                outputs: {self.default_outputs}
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                exclude:
                  - "excluded"
                outputs: {self.default_outputs}
              - type: sqs
                id: "{sqs_queue_arn}"
                exclude:
                  - "excluded"
                outputs: {self.default_outputs}
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                exclude:
                  - "excluded"
                outputs: {self.default_outputs}
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        events_cloudwatch_logs, _, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        second_call = handler(events_sqs, ctx)  # type:ignore

        assert second_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        third_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert third_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

        fourth_call = handler(events_kinesis, ctx)  # type:ignore

        assert fourth_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default", ignore_unavailable=True)
        assert self.elasticsearch.count(index="logs-generic-default", ignore_unavailable=True)["count"] == 0

        logstash_message = self.logstash.get_messages(expected=0)
        assert len(logstash_message) == 0

    def test_expand_event_from_list_empty_line(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-2.json")
        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")

        fixtures = [
            f"""{{"aField": [{first_expanded_event},{second_expanded_event}]}}\n"""
            f"""\n{{"aField": [{third_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 174
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(json_parser(third_expanded_event))
        assert logstash_message[2]["log"]["offset"] == 349
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_expand_event_from_list_empty_event_not_expanded(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-2.json")

        fixtures = [f"""{{"aField": [{first_expanded_event},"",{second_expanded_event}]}}"""]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "completed"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 233
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_no_dict_event(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = '"first_expanded_event"'
        second_expanded_event: str = '"second_expanded_event"'
        third_expanded_event: str = '"third_expanded_event"'

        fixtures = [
            f"""{{"firstRootField": "firstRootField", "secondRootField":"secondRootField",
            "aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: ["secondRootField"]
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == first_expanded_event
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == second_expanded_event
        assert logstash_message[1]["log"]["offset"] == 56
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == third_expanded_event
        assert logstash_message[2]["log"]["offset"] == 112
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_event_not_expanded(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        first_expanded_with_root_fields: dict[str, Any] = json_parser(first_expanded_event)
        first_expanded_with_root_fields["secondRootField"] = "secondRootField"

        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        second_expanded_with_root_fields: dict[str, Any] = json_parser(second_expanded_event)
        second_expanded_with_root_fields["secondRootField"] = "secondRootField"

        fixtures = [
            f"""{{"firstRootField": "firstRootField", "secondRootField":"secondRootField",
            "aField": [{first_expanded_event},{{}},{second_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: ["secondRootField"]
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "completed"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

        assert logstash_message[0]["message"] == json_dumper(first_expanded_with_root_fields)
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[1]["message"] == json_dumper(second_expanded_with_root_fields)
        assert logstash_message[1]["log"]["offset"] == 180
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_list(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        first_expanded_with_root_fields: dict[str, Any] = json_parser(first_expanded_event)
        first_expanded_with_root_fields["secondRootField"] = "secondRootField"

        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        second_expanded_with_root_fields: dict[str, Any] = json_parser(second_expanded_event)
        second_expanded_with_root_fields["secondRootField"] = "secondRootField"

        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event_with_root_fields: dict[str, Any] = json_parser(third_expanded_event)
        third_expanded_event_with_root_fields["secondRootField"] = "secondRootField"

        fixtures = [
            f"""{{"firstRootField": "firstRootField", "secondRootField":"secondRootField",
            "aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: ["secondRootField"]
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(first_expanded_with_root_fields)
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(second_expanded_with_root_fields)
        assert logstash_message[1]["log"]["offset"] == 114
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(third_expanded_event_with_root_fields)
        assert logstash_message[2]["log"]["offset"] == 228
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_list_no_fields_in_root(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        first_expanded_with_root_fields: dict[str, Any] = json_parser(first_expanded_event)
        first_expanded_with_root_fields["secondRootField"] = "secondRootField"

        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        second_expanded_with_root_fields: dict[str, Any] = json_parser(second_expanded_event)
        second_expanded_with_root_fields["secondRootField"] = "secondRootField"

        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event_with_root_fields: dict[str, Any] = json_parser(third_expanded_event)
        third_expanded_event_with_root_fields["secondRootField"] = "secondRootField"

        fixtures = [
            f"""{{"firstRootField": "firstRootField", "secondRootField":"secondRootField",
            "aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: ["secondRootField", "thirdRootField"]
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(first_expanded_with_root_fields)
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(second_expanded_with_root_fields)
        assert logstash_message[1]["log"]["offset"] == 114
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(third_expanded_event_with_root_fields)
        assert logstash_message[2]["log"]["offset"] == 228
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_all(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        first_expanded_with_root_fields: dict[str, Any] = json_parser(first_expanded_event)
        first_expanded_with_root_fields["firstRootField"] = "firstRootField"
        first_expanded_with_root_fields["secondRootField"] = "secondRootField"

        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        second_expanded_with_root_fields: dict[str, Any] = json_parser(second_expanded_event)
        second_expanded_with_root_fields["firstRootField"] = "firstRootField"
        second_expanded_with_root_fields["secondRootField"] = "secondRootField"

        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event_with_root_fields: dict[str, Any] = json_parser(third_expanded_event)
        third_expanded_event_with_root_fields["firstRootField"] = "firstRootField"
        third_expanded_event_with_root_fields["secondRootField"] = "secondRootField"

        fixtures = [
            f"""{{"firstRootField": "firstRootField", "secondRootField":"secondRootField",
            "aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: all
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(first_expanded_with_root_fields)
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(second_expanded_with_root_fields)
        assert logstash_message[1]["log"]["offset"] == 114
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(third_expanded_event_with_root_fields)
        assert logstash_message[2]["log"]["offset"] == 228
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_root_fields_to_add_to_expanded_event_all_no_fields_in_root(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        first_expanded_with_root_fields: dict[str, Any] = json_parser(first_expanded_event)

        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        second_expanded_with_root_fields: dict[str, Any] = json_parser(second_expanded_event)

        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event_with_root_fields: dict[str, Any] = json_parser(third_expanded_event)

        fixtures = [f"""{{"aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                root_fields_to_add_to_expanded_event: all
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(first_expanded_with_root_fields)
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(second_expanded_with_root_fields)
        assert logstash_message[1]["log"]["offset"] == 86
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(third_expanded_event_with_root_fields)
        assert logstash_message[2]["log"]["offset"] == 172
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_cloudwatch_logs_stream_as_input_instead_of_group(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        cloudwatch_stream_name_different = _time_based_id(suffix="source-stream-different")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name_different
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=[fixtures[0], fixtures[2]],
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name_different,
            messages_body=[fixtures[1]],
        )

        cloudwatch_group_arn = cloudwatch_group["arn"][0:-2]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        config_yaml: str = f"""
            inputs:
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}:log-stream:{cloudwatch_stream_name}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_cloudwatch_logs, event_ids_cloudwatch_logs, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        events_cloudwatch_logs_different, _, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name_different
        )

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert first_call == "completed"

        second_call = handler(events_cloudwatch_logs_different, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[0]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[0]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[0]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[1]["message"] == fixtures[2].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 0
        assert logstash_message[1]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[1]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[1]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[1]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[1]
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_cloudwatch_logs_no_input_defined(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=fixtures,
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]
        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        config_yaml: str = f"""
            inputs:
              - type: "cloudwatch-logs"
                id: "misconfigured-cloudwatch-logs"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_cloudwatch_logs, event_ids_cloudwatch_logs, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        ctx = ContextMock()
        first_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert first_call == "completed"

        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        replayed_messages = replayed_events["Records"]

        assert len(replayed_messages) == 3

        arn_components = cloudwatch_group_arn.split(":")
        arn_components[3] = "%AWS_REGION%"
        cloudwatch_group_arn = ":".join(arn_components)

        for message in replayed_messages:
            assert message["messageAttributes"]["originalEventSourceARN"]["stringValue"] == cloudwatch_group_arn

    def test_cloudwatch_logs_last_ending_offset_reset(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=fixtures,
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        config_yaml: str = f"""
            inputs:
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_cloudwatch_logs, event_ids_cloudwatch_logs, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        ctx = ContextMock()
        first_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[0]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[0]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[0]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == fixtures[1].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 0
        assert logstash_message[1]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[1]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[1]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[1]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[1]
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == fixtures[2].rstrip("\n")
        assert logstash_message[2]["log"]["offset"] == 0
        assert logstash_message[2]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[2]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[2]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[2]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[2]
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_cloudwatch_logs_last_event_expanded_offset_continue(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")

        fixtures = [f"""{{"aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""]

        cloudwatch_group_name = _time_based_id(suffix="source-group")
        cloudwatch_group = _logs_create_cloudwatch_logs_group(self.logs_client, group_name=cloudwatch_group_name)

        cloudwatch_stream_name = _time_based_id(suffix="source-stream")
        _logs_create_cloudwatch_logs_stream(
            self.logs_client, group_name=cloudwatch_group_name, stream_name=cloudwatch_stream_name
        )

        _logs_upload_event_to_cloudwatch_logs(
            self.logs_client,
            group_name=cloudwatch_group_name,
            stream_name=cloudwatch_stream_name,
            messages_body=fixtures,
        )

        cloudwatch_group_arn = cloudwatch_group["arn"]

        cloudwatch_group_name = cloudwatch_group_name
        cloudwatch_stream_name = cloudwatch_stream_name

        config_yaml: str = f"""
            inputs:
              - type: "cloudwatch-logs"
                id: "{cloudwatch_group_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_cloudwatch_logs, event_ids_cloudwatch_logs, _ = _logs_retrieve_event_from_cloudwatch_logs(
            self.logs_client, cloudwatch_group_name, cloudwatch_stream_name
        )

        ctx = ContextMock()
        first_call = handler(events_cloudwatch_logs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[0]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[0]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[0]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 86
        assert logstash_message[1]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[1]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[1]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[1]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(json_parser(third_expanded_event))
        assert logstash_message[2]["log"]["offset"] == 172
        assert logstash_message[2]["log"]["file"]["path"] == f"{cloudwatch_group_name}/{cloudwatch_stream_name}"
        assert logstash_message[2]["aws"]["cloudwatch"]["log_group"] == cloudwatch_group_name
        assert logstash_message[2]["aws"]["cloudwatch"]["log_stream"] == cloudwatch_stream_name
        assert logstash_message[2]["aws"]["cloudwatch"]["event_id"] == event_ids_cloudwatch_logs[0]
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_kinesis_data_stream_no_input_defined(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, fixtures)

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "misconfigured-id"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock()
        first_call = handler(events_kinesis, ctx)  # type:ignore

        assert first_call == "completed"

        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        replayed_messages = replayed_events["Records"]

        assert len(replayed_messages) == 3

        for message in replayed_messages:
            assert kinesis_stream_arn == message["messageAttributes"]["originalEventSourceARN"]["stringValue"]

    def test_kinesis_data_stream_last_ending_offset_reset(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, fixtures)

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock()
        first_call = handler(events_kinesis, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[0]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[0]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[0]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[0]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == fixtures[1].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 0
        assert logstash_message[1]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[1]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[1]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[1]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[1]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][1]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == fixtures[2].rstrip("\n")
        assert logstash_message[2]["log"]["offset"] == 0
        assert logstash_message[2]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[2]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[2]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[2]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[2]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][2]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_kinesis_data_stream_last_event_expanded_offset_continue(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")

        fixtures = [f"""{{"aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""]

        kinesis_stream_name = _time_based_id(suffix="source-kinesis")
        kinesis_stream = _kinesis_create_stream(self.kinesis_client, kinesis_stream_name)
        kinesis_stream_arn = kinesis_stream["StreamDescription"]["StreamARN"]

        _kinesis_put_records(self.kinesis_client, kinesis_stream_name, fixtures)

        config_yaml: str = f"""
            inputs:
              - type: "kinesis-data-stream"
                id: "{kinesis_stream_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_kinesis, _ = _kinesis_retrieve_event_from_kinesis_stream(
            self.kinesis_client, kinesis_stream_name, kinesis_stream_arn
        )

        ctx = ContextMock()
        first_call = handler(events_kinesis, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[0]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[0]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[0]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[0]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 86
        assert logstash_message[1]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[1]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[1]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[1]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[1]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(json_parser(third_expanded_event))
        assert logstash_message[2]["log"]["offset"] == 172
        assert logstash_message[2]["log"]["file"]["path"] == kinesis_stream_arn
        assert logstash_message[2]["aws"]["kinesis"]["type"] == "stream"
        assert logstash_message[2]["aws"]["kinesis"]["partition_key"] == "PartitionKey"
        assert logstash_message[2]["aws"]["kinesis"]["name"] == kinesis_stream_name
        assert (
            logstash_message[2]["aws"]["kinesis"]["sequence_number"]
            == events_kinesis["Records"][0]["kinesis"]["sequenceNumber"]
        )
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_sqs_no_input_defined(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]

        for fixture in fixtures:
            _sqs_send_messages(self.sqs_client, sqs_queue_url, fixture)

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "misconfigured-id"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        messages_sqs = events_sqs["Records"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "completed"

        replayed_events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        replayed_messages = replayed_events["Records"]

        assert len(messages_sqs) == 3
        assert len(replayed_messages) == 3
        for i, message in enumerate(replayed_messages):
            assert (
                messages_sqs[i]["eventSourceARN"]
                == message["messageAttributes"]["originalEventSourceARN"]["stringValue"]
            )

    def test_sqs_last_ending_offset_reset(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == fixtures[1].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 94
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == fixtures[2].rstrip("\n")
        assert logstash_message[2]["log"]["offset"] == 332
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_sqs_last_event_expanded_offset_continue(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")

        fixtures = [f"""{{"aField": [{first_expanded_event},{second_expanded_event},{third_expanded_event}]}}"""]

        sqs_queue_name = _time_based_id(suffix="source-sqs")

        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        config_yaml: str = f"""
            inputs:
              - type: "sqs"
                id: "{sqs_queue_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_sqs, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        message_id = events_sqs["Records"][0]["messageId"]

        ctx = ContextMock()
        first_call = handler(events_sqs, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert logstash_message[0]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[0]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[0]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "us-east-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 86
        assert logstash_message[1]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[1]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[1]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "us-east-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == json_dumper(json_parser(third_expanded_event))
        assert logstash_message[2]["log"]["offset"] == 172
        assert logstash_message[2]["log"]["file"]["path"] == sqs_queue_url_path
        assert logstash_message[2]["aws"]["sqs"]["name"] == sqs_queue_name
        assert logstash_message[2]["aws"]["sqs"]["message_id"] == message_id
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "us-east-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_s3_sqs_last_ending_offset_reset(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
            _load_file_fixture("cloudwatch-log-3.json"),
        ]

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress("".join(fixtures).encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])

        config_yaml: str = f"""
            inputs:
              - type: "s3-sqs"
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        ctx = ContextMock()
        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == fixtures[0].rstrip("\n")
        assert logstash_message[0]["log"]["offset"] == 0
        assert (
            logstash_message[0]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert logstash_message[0]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[0]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[0]["aws"]["s3"]["object"]["key"] == first_filename
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3

        assert logstash_message[1]["message"] == fixtures[1].rstrip("\n")
        assert logstash_message[1]["log"]["offset"] == 94
        assert (
            logstash_message[1]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert logstash_message[1]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[1]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[1]["aws"]["s3"]["object"]["key"] == first_filename
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert logstash_message[2]["message"] == fixtures[2].rstrip("\n")
        assert logstash_message[2]["log"]["offset"] == 332
        assert (
            logstash_message[2]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert logstash_message[2]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[2]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[2]["aws"]["s3"]["object"]["key"] == first_filename
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_s3_sqs_last_event_expanded_offset_continue(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        first_expanded_event: str = _load_file_fixture("cloudwatch-log-1.json")
        second_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")
        third_expanded_event: str = _load_file_fixture("cloudwatch-log-3.json")

        fixtures = [
            f"""{{"aField": [{first_expanded_event},{second_expanded_event}]}}""",
            f"""{{"aField": [{third_expanded_event}]}}""",
        ]

        s3_bucket_name = _time_based_id(suffix="test-bucket")
        first_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000000.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[0].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=first_filename,
        )

        second_filename = "exportedlog/uuid/yyyy-mm-dd-[$LATEST]hash/000001.gz"
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[1].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=second_filename,
        )

        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [first_filename])
        _sqs_send_s3_notifications(self.sqs_client, s3_sqs_queue_url, s3_bucket_name, [second_filename])

        config_yaml: str = f"""
            inputs:
              - type: "s3-sqs"
                id: "{s3_sqs_queue_arn}"
                expand_event_list_from_field: aField
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        events_s3, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        ctx = ContextMock()
        first_call = handler(events_s3, ctx)  # type:ignore

        assert first_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=1)
        assert len(logstash_message) == 1

        assert logstash_message[0]["message"] == json_dumper(json_parser(first_expanded_event))
        assert logstash_message[0]["log"]["offset"] == 0
        assert (
            logstash_message[0]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert logstash_message[0]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[0]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[0]["aws"]["s3"]["object"]["key"] == first_filename
        assert logstash_message[0]["cloud"]["provider"] == "aws"
        assert logstash_message[0]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[0]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[0]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        second_call = handler(continued_events, ctx)  # type:ignore

        assert second_call == "continuing"

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2

        assert logstash_message[1]["message"] == json_dumper(json_parser(second_expanded_event))
        assert logstash_message[1]["log"]["offset"] == 91
        assert (
            logstash_message[1]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{first_filename}"
        )
        assert logstash_message[1]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[1]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[1]["aws"]["s3"]["object"]["key"] == first_filename
        assert logstash_message[1]["cloud"]["provider"] == "aws"
        assert logstash_message[1]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[1]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[1]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        continued_events, _ = _sqs_get_messages(
            self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn
        )
        third_call = handler(continued_events, ctx)  # type:ignore

        assert third_call == "completed"

        logstash_message = self.logstash.get_messages(expected=3)
        assert len(logstash_message) == 3
        assert logstash_message[2]["message"] == json_dumper(json_parser(third_expanded_event))
        assert logstash_message[2]["log"]["offset"] == 0
        assert (
            logstash_message[2]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{second_filename}"
        )
        assert logstash_message[2]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert logstash_message[2]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert logstash_message[2]["aws"]["s3"]["object"]["key"] == second_filename
        assert logstash_message[2]["cloud"]["provider"] == "aws"
        assert logstash_message[2]["cloud"]["region"] == "eu-central-1"
        assert logstash_message[2]["cloud"]["account"]["id"] == "000000000000"
        assert logstash_message[2]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_cloud_trail_race(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())

        s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]

        config_yaml: str = f"""
            inputs:
              - type: s3-sqs
                id: "{s3_sqs_queue_arn}"
                tags: {self.default_tags}
                outputs: {self.default_outputs}
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            '{"Records": [' + _load_file_fixture("cloudwatch-log-2.json") + "]}",
        ]

        cloudtrail_filename_digest = (
            "AWSLogs/aws-account-id/CloudTrail-Digest/region/yyyy/mm/dd/"
            "aws-account-id_CloudTrail-Digest_region_end-time_random-string.log.gz"
        )
        cloudtrail_filename_non_digest = (
            "AWSLogs/aws-account-id/CloudTrail/region/yyyy/mm/dd/"
            "aws-account-id_CloudTrail_region_end-time_random-string.log.gz"
        )

        s3_bucket_name = _time_based_id(suffix="test-bucket")

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[0].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=cloudtrail_filename_digest,
        )

        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=gzip.compress(fixtures[1].encode("utf-8")),
            content_type="application/x-gzip",
            bucket_name=s3_bucket_name,
            key=cloudtrail_filename_non_digest,
        )

        _sqs_send_s3_notifications(
            self.sqs_client,
            s3_sqs_queue_url,
            s3_bucket_name,
            [cloudtrail_filename_digest, cloudtrail_filename_non_digest],
        )

        event, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-aws.cloudtrail-default")
        assert self.elasticsearch.count(index="logs-aws.cloudtrail-default")["count"] == 2

        res = self.elasticsearch.search(index="logs-aws.cloudtrail-default", sort="_seq_no")
        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}

        assert res["hits"]["hits"][0]["_source"]["message"] == fixtures[0].rstrip("\n")
        assert res["hits"]["hits"][0]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][0]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_digest}"
        )
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_digest
        assert res["hits"]["hits"][0]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][0]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][0]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]

        assert res["hits"]["hits"][1]["_source"]["message"] == json_dumper(
            json_parser(_load_file_fixture("cloudwatch-log-2.json").rstrip("\n"))
        )
        assert res["hits"]["hits"][1]["_source"]["log"]["offset"] == 0
        assert (
            res["hits"]["hits"][1]["_source"]["log"]["file"]["path"]
            == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_non_digest}"
        )
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_non_digest
        assert res["hits"]["hits"][1]["_source"]["cloud"]["provider"] == "aws"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["region"] == "eu-central-1"
        assert res["hits"]["hits"][1]["_source"]["cloud"]["account"]["id"] == "000000000000"
        assert res["hits"]["hits"][1]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]

        logstash_message = self.logstash.get_messages(expected=2)
        assert len(logstash_message) == 2
        res["hits"]["hits"][0]["_source"]["tags"].remove("aws-cloudtrail")
        res["hits"]["hits"][1]["_source"]["tags"].remove("aws-cloudtrail")

        assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]

        assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]

    def test_es_ssl_fingerprint_mismatch(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}:AA
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        ctx = ContextMock()

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        message_id = event["Records"][0]["messageId"]

        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "continuing"

        assert self.elasticsearch.exists(index="logs-generic-default") is False

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        second_call = handler(event, ctx)  # type:ignore

        assert second_call == "continuing"

        assert self.elasticsearch.exists(index="logs-generic-default") is False

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        third_call = handler(event, ctx)  # type:ignore

        assert third_call == "completed"

        assert self.elasticsearch.exists(index="logs-generic-default") is False

        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert len(events["Records"]) == 2

        first_body: dict[str, Any] = json_parser(events["Records"][0]["body"])
        second_body: dict[str, Any] = json_parser(events["Records"][1]["body"])

        assert first_body["event_payload"]["message"] == fixtures[0].rstrip("\n")
        assert first_body["event_payload"]["log"]["offset"] == 0
        assert first_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert first_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert first_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert first_body["event_payload"]["cloud"]["provider"] == "aws"
        assert first_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert first_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert first_body["event_payload"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        assert second_body["event_payload"]["message"] == fixtures[1].rstrip("\n")
        assert second_body["event_payload"]["log"]["offset"] == 94
        assert second_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert second_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert second_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert second_body["event_payload"]["cloud"]["provider"] == "aws"
        assert second_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert second_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert second_body["event_payload"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

    def test_es_no_matching_action_failed(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]

        message: str = "a message"
        fingerprint: str = "DUEwoALOve1Y9MtPCfT7IJGU3IQ="

        # Create an expected id so that es.send will fail
        self.elasticsearch.index(
            index="logs-generic-default",
            op_type="create",
            id=fingerprint,
            document={"@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
        )

        processors = {
            "processors": [
                {
                    "fingerprint": {
                        "fields": ["message"],
                        "target_field": "_id",
                    }
                }
            ]
        }

        # Add a pipeline that will generate the same _id
        self.elasticsearch.put_pipeline(id="id_fingerprint_pipeline", body=processors)
        self.elasticsearch.put_settings(
            index="logs-generic-default", body={"index.default_pipeline": "id_fingerprint_pipeline"}
        )

        self.elasticsearch.refresh(index="logs-generic-default")

        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 1

        _sqs_send_messages(self.sqs_client, sqs_queue_url, message)

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)

        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")

        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 1

        res = self.elasticsearch.search(index="logs-generic-default")
        assert "message" not in res["hits"]["hits"][0]["_source"]

        event, timestamp = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert not event["Records"]
        assert not timestamp

    def test_ls_ssl_fingerprint_mimsmatch(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}:AA
                      username: "{self.logstash.logstash_user}"
                      password: "{self.logstash.logstash_password}"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        ctx = ContextMock()

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        message_id = event["Records"][0]["messageId"]

        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "continuing"

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        second_call = handler(event, ctx)  # type:ignore

        assert second_call == "continuing"

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        third_call = handler(event, ctx)  # type:ignore

        assert third_call == "completed"

        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert len(events["Records"]) == 2

        first_body: dict[str, Any] = json_parser(events["Records"][0]["body"])
        second_body: dict[str, Any] = json_parser(events["Records"][1]["body"])

        assert first_body["event_payload"]["message"] == fixtures[0].rstrip("\n")
        assert first_body["event_payload"]["log"]["offset"] == 0
        assert first_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert first_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert first_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert first_body["event_payload"]["cloud"]["provider"] == "aws"
        assert first_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert first_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert first_body["event_payload"]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert second_body["event_payload"]["message"] == fixtures[1].rstrip("\n")
        assert second_body["event_payload"]["log"]["offset"] == 94
        assert second_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert second_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert second_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert second_body["event_payload"]["cloud"]["provider"] == "aws"
        assert second_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert second_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert second_body["event_payload"]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_ls_wrong_auth_creds(self) -> None:
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "logstash"
                    args:
                      logstash_url: "{self.logstash.get_url()}"
                      ssl_assert_fingerprint: {self.logstash.ssl_assert_fingerprint}
                      username: "wrong_username"
                      password: "wrong_password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        ctx = ContextMock()

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
            _load_file_fixture("cloudwatch-log-2.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        message_id = event["Records"][0]["messageId"]

        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "continuing"

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        second_call = handler(event, ctx)  # type:ignore

        assert second_call == "continuing"

        event, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_CONTINUE_URL"], self.sqs_continue_queue_arn)
        third_call = handler(event, ctx)  # type:ignore

        assert third_call == "completed"

        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert len(events["Records"]) == 2

        first_body: dict[str, Any] = json_parser(events["Records"][0]["body"])
        second_body: dict[str, Any] = json_parser(events["Records"][1]["body"])

        assert first_body["event_payload"]["message"] == fixtures[0].rstrip("\n")
        assert first_body["event_payload"]["log"]["offset"] == 0
        assert first_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert first_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert first_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert first_body["event_payload"]["cloud"]["provider"] == "aws"
        assert first_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert first_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert first_body["event_payload"]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

        assert second_body["event_payload"]["message"] == fixtures[1].rstrip("\n")
        assert second_body["event_payload"]["log"]["offset"] == 94
        assert second_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert second_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert second_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert second_body["event_payload"]["cloud"]["provider"] == "aws"
        assert second_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert second_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert second_body["event_payload"]["tags"] == ["forwarded", "tag1", "tag2", "tag3"]

    def test_es_version_conflict_exception(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        # Index event a second time to trigger version conflict
        second_call = handler(event, ctx)  # type:ignore

        assert second_call == "completed"

        self.elasticsearch.refresh(index="logs-generic-default")

        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 1

        # Test no duplicate events end in the replay queue
        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert len(events["Records"]) == 0

    def test_es_dead_letter_index(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        dead_letter_index_name = "logs-generic-default-dli"

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      es_dead_letter_index: "{dead_letter_index_name}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        message_id = event["Records"][0]["messageId"]

        # Create pipeline to reject documents
        processors = {
            "processors": [
                {
                    "fail": {
                        "message": "test_es_non_indexable_dead_letter_index fail message",
                    }
                },
            ]
        }

        self.elasticsearch.put_pipeline(id="test_es_non_indexable_dead_letter_index_fail_pipeline", body=processors)

        self.elasticsearch.create_data_stream(name="logs-generic-default")
        self.elasticsearch.put_settings(
            index="logs-generic-default",
            body={"index.default_pipeline": "test_es_non_indexable_dead_letter_index_fail_pipeline"},
        )

        self.elasticsearch.refresh(index="logs-generic-default")

        self.elasticsearch.create_data_stream(name=dead_letter_index_name)

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        # Test document has been rejected from target index
        self.elasticsearch.refresh(index="logs-generic-default")

        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 0

        # Test document has been redirected to dli
        assert self.elasticsearch.exists(index=dead_letter_index_name) is True

        self.elasticsearch.refresh(index=dead_letter_index_name)

        assert self.elasticsearch.count(index=dead_letter_index_name)["count"] == 1

        res = self.elasticsearch.search(index=dead_letter_index_name, sort="_seq_no")

        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}

        assert (
            res["hits"]["hits"][0]["_source"]["error"]["reason"]
            == "test_es_non_indexable_dead_letter_index fail message"
        )
        assert res["hits"]["hits"][0]["_source"]["error"]["type"] == "fail_processor_exception"
        dead_letter_message = json_parser(res["hits"]["hits"][0]["_source"]["message"])
        assert dead_letter_message["log"]["offset"] == 0
        assert dead_letter_message["log"]["file"]["path"] == sqs_queue_url_path
        assert dead_letter_message["aws"]["sqs"]["name"] == sqs_queue_name
        assert dead_letter_message["aws"]["sqs"]["message_id"] == message_id
        assert dead_letter_message["cloud"]["provider"] == "aws"
        assert dead_letter_message["cloud"]["region"] == "us-east-1"
        assert dead_letter_message["cloud"]["account"]["id"] == "000000000000"
        assert dead_letter_message["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]

        # Test event does not go into the replay queue
        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)

        assert len(events["Records"]) == 0

    def test_es_non_indexable_dead_letter_index(self) -> None:
        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        sqs_queue_name = _time_based_id(suffix="source-sqs")
        sqs_queue = _sqs_create_queue(self.sqs_client, sqs_queue_name, self.localstack.get_url())

        dead_letter_index_name = "logs-generic-default-dli"

        sqs_queue_arn = sqs_queue["QueueArn"]
        sqs_queue_url = sqs_queue["QueueUrl"]
        sqs_queue_url_path = sqs_queue["QueueUrlPath"]

        config_yaml: str = f"""
            inputs:
              - type: sqs
                id: "{sqs_queue_arn}"
                tags: {self.default_tags}
                outputs:
                  - type: "elasticsearch"
                    args:
                      elasticsearch_url: "{self.elasticsearch.get_url()}"
                      es_dead_letter_index: "{dead_letter_index_name}"
                      ssl_assert_fingerprint: {self.elasticsearch.ssl_assert_fingerprint}
                      username: "{self.secret_arn}:username"
                      password: "{self.secret_arn}:password"
        """

        config_file_path = "config.yaml"
        config_bucket_name = _time_based_id(suffix="config-bucket")
        _s3_upload_content_to_bucket(
            client=self.s3_client,
            content=config_yaml.encode("utf-8"),
            content_type="text/plain",
            bucket_name=config_bucket_name,
            key=config_file_path,
        )

        os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"

        fixtures = [
            _load_file_fixture("cloudwatch-log-1.json"),
        ]

        _sqs_send_messages(self.sqs_client, sqs_queue_url, "".join(fixtures))

        event, _ = _sqs_get_messages(self.sqs_client, sqs_queue_url, sqs_queue_arn)
        message_id = event["Records"][0]["messageId"]

        # Create pipeline to reject documents
        processors = {
            "processors": [
                {
                    "fail": {
                        "message": "test_es_non_indexable_dead_letter_index",
                    }
                },
            ]
        }

        self.elasticsearch.put_pipeline(id="test_es_non_indexable_dead_letter_index_fail_pipeline", body=processors)

        self.elasticsearch.create_data_stream(name="logs-generic-default")
        self.elasticsearch.put_settings(
            index="logs-generic-default",
            body={"index.default_pipeline": "test_es_non_indexable_dead_letter_index_fail_pipeline"},
        )

        self.elasticsearch.refresh(index="logs-generic-default")

        self.elasticsearch.create_data_stream(name=dead_letter_index_name)
        self.elasticsearch.put_settings(
            index=dead_letter_index_name,
            body={"index.default_pipeline": "test_es_non_indexable_dead_letter_index_fail_pipeline"},
        )

        ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        assert first_call == "completed"

        # Test document has been rejected from target index
        self.elasticsearch.refresh(index="logs-generic-default")

        assert self.elasticsearch.count(index="logs-generic-default")["count"] == 0

        # Test event does not go into the dead letter queue
        assert self.elasticsearch.exists(index=dead_letter_index_name) is True

        self.elasticsearch.refresh(index=dead_letter_index_name)

        assert self.elasticsearch.count(index=dead_letter_index_name)["count"] == 0

        # Test event has been redirected into the replay queue
        events, _ = _sqs_get_messages(self.sqs_client, os.environ["SQS_REPLAY_URL"], self.sqs_replay_queue_arn)
        assert len(events["Records"]) == 1

        first_body: dict[str, Any] = json_parser(events["Records"][0]["body"])

        assert first_body["event_payload"]["message"] == fixtures[0].rstrip("\n")
        assert first_body["event_payload"]["log"]["offset"] == 0
        assert first_body["event_payload"]["log"]["file"]["path"] == sqs_queue_url_path
        assert first_body["event_payload"]["aws"]["sqs"]["name"] == sqs_queue_name
        assert first_body["event_payload"]["aws"]["sqs"]["message_id"] == message_id
        assert first_body["event_payload"]["cloud"]["provider"] == "aws"
        assert first_body["event_payload"]["cloud"]["region"] == "us-east-1"
        assert first_body["event_payload"]["cloud"]["account"]["id"] == "000000000000"
        assert first_body["event_payload"]["tags"] == ["forwarded", "generic", "tag1", "tag2", "tag3"]
