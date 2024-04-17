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
        print("TEST LS ES OUTPUT.")

        assert isinstance(self.elasticsearch, ElasticsearchContainer)
        assert isinstance(self.logstash, LogstashContainer)
        assert isinstance(self.localstack, LocalStackContainer)

        s3_sqs_queue_name = _time_based_id(suffix="source-s3-sqs")

        #s3_sqs_queue = _sqs_create_queue(self.sqs_client, s3_sqs_queue_name, self.localstack.get_url())
#
        #s3_sqs_queue_arn = s3_sqs_queue["QueueArn"]
        #s3_sqs_queue_url = s3_sqs_queue["QueueUrl"]
#
        #config_yaml: str = f"""
        #    inputs:
        #      - type: s3-sqs
        #        id: "{s3_sqs_queue_arn}"
        #        tags: {self.default_tags}
        #        outputs: {self.default_outputs}
        #"""

        #config_file_path = "config.yaml"
        #config_bucket_name = _time_based_id(suffix="config-bucket")
        #_s3_upload_content_to_bucket(
        #    client=self.s3_client,
        #    content=config_yaml.encode("utf-8"),
        #    content_type="text/plain",
        #    bucket_name=config_bucket_name,
        #    key=config_file_path,
        #)
#
        #os.environ["S3_CONFIG_FILE"] = f"s3://{config_bucket_name}/{config_file_path}"
        #fixtures = [
        #    _load_file_fixture("cloudwatch-log-1.json"),
        #    _load_file_fixture("cloudwatch-log-2.json"),
        #]
#
        #cloudtrail_filename_digest = (
        #    "AWSLogs/aws-account-id/CloudTrail-Digest/region/yyyy/mm/dd/"
        #    "aws-account-id_CloudTrail-Digest_region_end-time_random-string.log.gz"
        #)
        #cloudtrail_filename_non_digest = (
        #    "AWSLogs/aws-account-id/CloudTrail/region/yyyy/mm/dd/"
        #    "aws-account-id_CloudTrail_region_end-time_random-string.log.gz"
        #)
#
        #s3_bucket_name = _time_based_id(suffix="test-bucket")
#
        #_s3_upload_content_to_bucket(
        #    client=self.s3_client,
        #    content=gzip.compress(fixtures[0].encode("utf-8")),
        #    content_type="application/x-gzip",
        #    bucket_name=s3_bucket_name,
        #    key=cloudtrail_filename_digest,
        #)
#
        #_s3_upload_content_to_bucket(
        #    client=self.s3_client,
        #    content=gzip.compress(fixtures[1].encode("utf-8")),
        #    content_type="application/x-gzip",
        #    bucket_name=s3_bucket_name,
        #    key=cloudtrail_filename_non_digest,
        #)
#
        #_sqs_send_s3_notifications(
        #    self.sqs_client,
        #    s3_sqs_queue_url,
        #    s3_bucket_name,
        #    [cloudtrail_filename_digest, cloudtrail_filename_non_digest],
        #)
#
        #event, _ = _sqs_get_messages(self.sqs_client, s3_sqs_queue_url, s3_sqs_queue_arn)
#
        #ctx = ContextMock(remaining_time_in_millis=_OVER_COMPLETION_GRACE_PERIOD_2m)
        first_call = handler(event, ctx)  # type:ignore

        #assert first_call == "completed"
#
        #self.elasticsearch.refresh(index="logs-aws.cloudtrail-default")
        #assert self.elasticsearch.count(index="logs-aws.cloudtrail-default")["count"] == 2
#
        #res = self.elasticsearch.search(index="logs-aws.cloudtrail-default", sort="_seq_no")
        #assert res["hits"]["total"] == {"value": 2, "relation": "eq"}
#
        #assert res["hits"]["hits"][0]["_source"]["message"] == fixtures[0].rstrip("\n")
        #assert res["hits"]["hits"][0]["_source"]["log"]["offset"] == 0
        #assert (
        #    res["hits"]["hits"][0]["_source"]["log"]["file"]["path"]
        #    == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_digest}"
        #)
        #assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        #assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        #assert res["hits"]["hits"][0]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_digest
        #assert res["hits"]["hits"][0]["_source"]["cloud"]["provider"] == "aws"
        #assert res["hits"]["hits"][0]["_source"]["cloud"]["region"] == "eu-central-1"
        #assert res["hits"]["hits"][0]["_source"]["cloud"]["account"]["id"] == "000000000000"
        #assert res["hits"]["hits"][0]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]
#
        #assert res["hits"]["hits"][1]["_source"]["message"] == fixtures[1].rstrip("\n")
        #assert res["hits"]["hits"][1]["_source"]["log"]["offset"] == 0
        #assert (
        #    res["hits"]["hits"][1]["_source"]["log"]["file"]["path"]
        #    == f"https://{s3_bucket_name}.s3.eu-central-1.amazonaws.com/{cloudtrail_filename_non_digest}"
        #)
        #assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["name"] == s3_bucket_name
        #assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["bucket"]["arn"] == f"arn:aws:s3:::{s3_bucket_name}"
        #assert res["hits"]["hits"][1]["_source"]["aws"]["s3"]["object"]["key"] == cloudtrail_filename_non_digest
        #assert res["hits"]["hits"][1]["_source"]["cloud"]["provider"] == "aws"
        #assert res["hits"]["hits"][1]["_source"]["cloud"]["region"] == "eu-central-1"
        #assert res["hits"]["hits"][1]["_source"]["cloud"]["account"]["id"] == "000000000000"
        #assert res["hits"]["hits"][1]["_source"]["tags"] == ["forwarded", "aws-cloudtrail", "tag1", "tag2", "tag3"]
#
        #logstash_message = self.logstash.get_messages(expected=2)
        #assert len(logstash_message) == 2
        #res["hits"]["hits"][0]["_source"]["tags"].remove("aws-cloudtrail")
        #res["hits"]["hits"][1]["_source"]["tags"].remove("aws-cloudtrail")
#
        #assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        #assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        #assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        #assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        #assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]
#
        #assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        #assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        #assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        #assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        #assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]
#
        #self.elasticsearch.refresh(index="logs-stash.elasticsearch-output")
        #assert self.elasticsearch.count(index="logs-stash.elasticsearch-output")["count"] == 2
#
        #res = self.elasticsearch.search(index="logs-stash.elasticsearch-output", sort="_seq_no")
        #assert res["hits"]["total"] == {"value": 2, "relation": "eq"}
#
        #assert res["hits"]["hits"][0]["_source"]["aws"] == logstash_message[0]["aws"]
        #assert res["hits"]["hits"][0]["_source"]["cloud"] == logstash_message[0]["cloud"]
        #assert res["hits"]["hits"][0]["_source"]["log"] == logstash_message[0]["log"]
        #assert res["hits"]["hits"][0]["_source"]["message"] == logstash_message[0]["message"]
        #assert res["hits"]["hits"][0]["_source"]["tags"] == logstash_message[0]["tags"]
#
        #assert res["hits"]["hits"][1]["_source"]["aws"] == logstash_message[1]["aws"]
        #assert res["hits"]["hits"][1]["_source"]["cloud"] == logstash_message[1]["cloud"]
        #assert res["hits"]["hits"][1]["_source"]["log"] == logstash_message[1]["log"]
        #assert res["hits"]["hits"][1]["_source"]["message"] == logstash_message[1]["message"]
        #assert res["hits"]["hits"][1]["_source"]["tags"] == logstash_message[1]["tags"]
#
