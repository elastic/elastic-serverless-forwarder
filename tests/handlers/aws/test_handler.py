# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
import json
import os
from copy import deepcopy
from typing import Any, Union
from unittest import TestCase

import docker
import mock
from elasticsearch import Elasticsearch
from localstack.services import infra
from localstack.utils import testutil
from localstack.utils.aws import aws_stack

from handlers.aws import lambda_handler


class ContextMock:
    aws_request_id = "aws_request_id"
    invoked_function_arn = "invoked:function:arn:invoked:function:arn"

    @staticmethod
    def get_remaining_time_in_millis() -> int:
        return 0


class TestLambdaHandler(TestCase):
    def _event_from_sqs_message(self) -> dict[str, Any]:
        sqs_client = aws_stack.connect_to_service("sqs")
        messages = sqs_client.receive_message(
            QueueUrl=self._continuing_queue_info["QueueUrl"], MaxNumberOfMessages=2, MessageAttributeNames=["All"]
        )

        assert "Messages" in messages
        assert len(messages["Messages"]) == 1

        message = messages["Messages"][0]
        message["body"] = message["Body"]
        message["messageAttributes"] = message["MessageAttributes"]
        for attribute in message["messageAttributes"]:
            new_attribute = deepcopy(message["messageAttributes"][attribute])
            for attribute_key in message["messageAttributes"][attribute]:
                camel_case_key = "".join([attribute_key[0].lower(), attribute_key[1:]])
                new_attribute[camel_case_key] = new_attribute[attribute_key]
                message["messageAttributes"][attribute] = new_attribute

        message["eventSource"] = "aws:sqs"
        message["eventSourceARN"] = self._continuing_queue_info["QueueArn"]
        return dict(Records=[message])

    @staticmethod
    def _upload_content_to_bucket(
        content: Union[bytes, str], content_type: str, bucket_name: str, key_name: str
    ) -> None:
        client = aws_stack.connect_to_service("s3")

        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=key_name, Body=content, ContentType=content_type)

    def setUp(self) -> None:
        docker_client = docker.from_env()
        try:
            self._docket_network = docker_client.networks.get("run_tests")
        except Exception:
            self._docket_network = docker_client.networks.create("run_tests")

        self._elastic_container = docker_client.containers.run(
            "docker.elastic.co/elasticsearch/elasticsearch:7.15.1",
            detach=True,
            environment=["ELASTIC_PASSWORD=password", "discovery.type=single-node"],
            network="run_tests",
            ports={"9200/tcp": None},
        )

        while len(self._elastic_container.ports) == 0:
            self._elastic_container = docker_client.containers.get(self._elastic_container.id)

        os.environ["LAMBDA_EXECUTOR"] = "local"
        infra.start_infra(asynchronous=True)

        self._source_queue_info = testutil.create_sqs_queue("source-queue")
        self._continuing_queue_info = testutil.create_sqs_queue("continuing-queue")

        es_host_ip: str = self._elastic_container.ports["9200/tcp"][0]["HostIp"]
        es_host_port: str = self._elastic_container.ports["9200/tcp"][0]["HostPort"]
        config_yaml: str = f"""
        inputs:
          - type: "sqs"
            id: "{self._source_queue_info["QueueArn"]}"
            outputs:
              - type: "elasticsearch"
                args:
                  hosts:
                    - "{es_host_ip}:{es_host_port}"
                  scheme: "http"
                  username: "elastic"
                  password: "password"
                  dataset: "redis.log"
                  namespace: "default"
                """
        self._upload_content_to_bucket(
            content=config_yaml, content_type="text/plain", bucket_name="config-bucket", key_name="config.yaml"
        )

        redis_log: bytes = (
            "79191:C 08 Jul 2021 13:25:02.609 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo\n"
            + "79191:C 08 Jul 2021 13:25:02.610 # Redis version=6.2.4, bits=64, commit=00000000, "
            + "modified=0, pid=79191, just started"
        ).encode("UTF-8")

        self._upload_content_to_bucket(
            content=gzip.compress(redis_log),
            content_type="application/x-gzip",
            bucket_name="test-bucket",
            key_name="redis.log.gz",
        )

        self._es_client = Elasticsearch(
            hosts=[f"{es_host_ip}:{es_host_port}"], scheme="http", http_auth=("elastic", "password")
        )

        os.environ["S3_CONFIG_FILE"] = "s3://config-bucket/config.yaml"
        os.environ["SQS_CONTINUE_URL"] = self._continuing_queue_info["QueueUrl"]

    def tearDown(self) -> None:
        infra.stop_infra()
        try:
            self._docket_network.remove()
        except Exception:
            pass

        self._elastic_container.stop()
        self._elastic_container.remove()

    @mock.patch("storage.S3Storage._s3_client", aws_stack.connect_to_service("s3"))
    @mock.patch("handlers.aws.sqs_trigger._get_sqs_client", lambda: aws_stack.connect_to_service("sqs"))
    def test_lambda_handler(self) -> None:
        ctx = ContextMock()
        event = {
            "Records": [
                {
                    "messageId": "9b745861-1171-489c-9748-799ed2a3d9da",
                    "body": json.dumps(
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
                                        "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                                        "object": {
                                            "key": "redis.log.gz",
                                        },
                                    },
                                }
                            ]
                        }
                    ),
                    "eventSource": "aws:sqs",
                    "eventSourceARN": self._source_queue_info["QueueArn"],
                },
            ]
        }

        first_call = lambda_handler(event, ctx)  # type:ignore

        assert first_call == "continuing"

        while self._es_client.count(index="logs-redis.log-default")["count"] < 1:
            pass

        res = self._es_client.search(index="logs-redis.log-default")
        assert res["hits"]["total"] == {"value": 1, "relation": "eq"}
        assert (
            res["hits"]["hits"][0]["_source"]["fields"]["message"]
            == "79191:C 08 Jul 2021 13:25:02.609 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo"
        )
        assert res["hits"]["hits"][0]["_source"]["fields"]["log"] == {
            "offset": 0,
            "file": {"path": "https://test-bucket.s3.eu-central-1.amazonaws.com/redis.log.gz"},
        }
        assert res["hits"]["hits"][0]["_source"]["fields"]["aws"] == {
            "s3": {
                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                "object": {"key": "redis.log.gz"},
            }
        }
        assert res["hits"]["hits"][0]["_source"]["fields"]["cloud"] == {"provider": "aws", "region": "eu-central-1"}

        event = self._event_from_sqs_message()
        second_call = lambda_handler(event, ctx)  # type:ignore

        assert second_call == "continuing"

        while self._es_client.count(index="logs-redis.log-default")["count"] < 2:
            pass

        res = self._es_client.search(index="logs-redis.log-default")
        assert res["hits"]["total"] == {"value": 2, "relation": "eq"}
        assert (
            res["hits"]["hits"][1]["_source"]["fields"]["message"]
            == "79191:C 08 Jul 2021 13:25:02.610 # Redis version=6.2.4, bits=64, commit=00000000, "
            + "modified=0, pid=79191, just started"
        )

        assert res["hits"]["hits"][1]["_source"]["fields"]["log"] == {
            "offset": 81,
            "file": {"path": "https://test-bucket.s3.eu-central-1.amazonaws.com/redis.log.gz"},
        }
        assert res["hits"]["hits"][1]["_source"]["fields"]["aws"] == {
            "s3": {
                "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                "object": {"key": "redis.log.gz"},
            }
        }
        assert res["hits"]["hits"][1]["_source"]["fields"]["cloud"] == {"provider": "aws", "region": "eu-central-1"}

        event = self._event_from_sqs_message()
        third_call = lambda_handler(event, ctx)  # type:ignore

        assert third_call == "completed"
