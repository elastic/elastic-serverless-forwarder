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
