# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.


import random
import string
from datetime import datetime
from typing import Any
from unittest import TestCase

import pytest

from handlers.aws.utils import (
    cloudwatch_logs_object_id,
    get_shipper_from_input,
    kinesis_record_id,
    s3_object_id,
    sqs_object_id,
)
from share import parse_config
from shippers import LogstashShipper

# Elasticsearch _id constraints
MAX_ES_ID_SIZ_BYTES = 512

# Kinesis Input
# https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html
MAX_STREAM_NAME_CHARS = 128
# https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#Streams-PutRecord-request-PartitionKey
MAX_PARTITION_KEY_CHARS = 256
# https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#API_PutRecord_ResponseSyntax
MAX_SEQUENCE_NUMBER_DIGITS = 128

# S3-SQS Input
# https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_CreateBucket.html
MAX_BUCKET_NAME_CHARS = 255
# https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
# S3 Object key does not seem to have a maximum allowed number of chars, so we set it to our internal maximum
MAX_OBJECT_KEY_CHARS = 512

# SQS Input
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#API_CreateQueue_RequestParameters
MAX_QUEUE_NAME_CHARS = 80
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html
MAX_MESSAGE_ID_CHARS = 100

# Cloudwatch logs input
# https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_CreateLogGroup.html
MAX_CW_LOG_GROUP_NAME_CHARS = 512
# https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_CreateLogStream.html
MAX_CW_LOG_STREAM_NAME_CHARS = 512
# No docs available, set it to the max
MAX_CW_EVENT_ID_CHARS = 512


def _utf8len(s: str) -> int:
    return len(s.encode("utf-8"))


def _get_random_string_of_size(size: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


def _get_random_digit_string_of_size(size: int) -> str:
    return "".join(random.choices(string.digits, k=size))


@pytest.mark.unit
class TestGetTriggerTypeAndConfigSource(TestCase):
    def test_get_trigger_type_and_config_source(self) -> None:
        from handlers.aws.utils import CONFIG_FROM_PAYLOAD, CONFIG_FROM_S3FILE, get_trigger_type_and_config_source

        with self.subTest("cloudwatch-logs and CONFIG_FROM_S3FILE"):
            event: dict[str, Any] = {"awslogs": {"data": ""}}

            assert get_trigger_type_and_config_source(event=event) == ("cloudwatch-logs", CONFIG_FROM_S3FILE)

        with self.subTest("no Records"):
            with self.assertRaisesRegexp(Exception, "Not supported trigger"):
                event = {}

                get_trigger_type_and_config_source(event=event)

        with self.subTest("len(Records) < 1"):
            with self.assertRaisesRegexp(Exception, "Not supported trigger"):
                event = {"Records": []}

                get_trigger_type_and_config_source(event=event)

        with self.subTest("body in first record: replay-sqs CONFIG_FROM_S3FILE"):
            event = {
                "Records": [
                    {
                        "body": '{"output_destination": "output_destination", '
                        '"output_args": "output_args", "event_payload": "event_payload"}'
                    }
                ]
            }

            assert get_trigger_type_and_config_source(event=event) == ("replay-sqs", CONFIG_FROM_S3FILE)

        with self.subTest("body in first record: eventSource override"):
            event = {"Records": [{"body": '{"Records": [{"eventSource":"aws:s3"}]}', "eventSource": "aws:kinesis"}]}

            assert get_trigger_type_and_config_source(event=event) == ("s3-sqs", CONFIG_FROM_S3FILE)

        with self.subTest("body in first record: eventSource not override"):
            event = {
                "Records": [
                    {"body": '{"Records": [{"eventSource":"not-available-trigger"}]}', "eventSource": "aws:kinesis"}
                ]
            }

            assert get_trigger_type_and_config_source(event=event) == ("kinesis-data-stream", CONFIG_FROM_S3FILE)

        with self.subTest("body not in first record: eventSource not override"):
            event = {"Records": [{"eventSource": "aws:kinesis"}]}

            assert get_trigger_type_and_config_source(event=event) == ("kinesis-data-stream", CONFIG_FROM_S3FILE)

        with self.subTest("messageAttributes without originalEventSourceARN in first record, CONFIG_FROM_S3FILE"):
            event = {"Records": [{"messageAttributes": {}, "eventSource": "aws:kinesis"}]}

            assert get_trigger_type_and_config_source(event=event) == ("kinesis-data-stream", CONFIG_FROM_S3FILE)

        with self.subTest("messageAttributes with originalEventSourceARN in first record, CONFIG_FROM_PAYLOAD"):
            event = {"Records": [{"messageAttributes": {"originalEventSourceARN": ""}, "eventSource": "aws:kinesis"}]}

            assert get_trigger_type_and_config_source(event=event) == ("kinesis-data-stream", CONFIG_FROM_PAYLOAD)


@pytest.mark.unit
class TestDiscoverIntegrationScope(TestCase):
    def test_discover_integration_scope(self) -> None:
        from handlers.aws.utils import discover_integration_scope

        with self.subTest("discover_integration_scope aws.cloudtrail integration scope"):
            s3_object_key = (
                "AWSLogs/aws-account-id/CloudTrail/region/"
                "yyyy/mm/dd/aws-account-id_CloudTrail_region_end-time_random-string.log.gz"
            )

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.cloudtrail"

        with self.subTest("discover_integration_scope aws.cloudtrail digest integration scope"):
            s3_object_key = (
                "AWSLogs/aws-account-id/CloudTrail-Digest/region/"
                "yyyy/mm/dd/aws-account-id_CloudTrail-Digest_region_end-time_random-string.log.gz"
            )

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.cloudtrail-digest"

        with self.subTest("discover_integration_scope aws.cloudtrail insight integration scope"):
            s3_object_key = (
                "AWSLogs/aws-account-id/CloudTrail-Insight/region/"
                "yyyy/mm/dd/aws-account-id_CloudTrail-Insight_region_end-time_random-string.log.gz"
            )

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.cloudtrail"

        with self.subTest("discover_integration_scope aws.cloudwatch_logs integration scope"):
            s3_object_key = "exportedlogs/111-222-333/2021-12-28/hash/file.gz"

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.cloudwatch_logs"

        with self.subTest("discover_integration_scope aws.elb_logs integration scope"):
            s3_object_key = (
                "AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/"
                "aws-account-id_elasticloadbalancing_region_load-balancer-id_end-time_ip-address_random-string.log.gz"
            )

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.elb_logs"

        with self.subTest("discover_integration_scope aws.firewall_logs integration scope"):
            s3_object_key = "AWSLogs/aws-account-id/network-firewall/log-type/Region/firewall-name/timestamp/"

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.firewall_logs"

        with self.subTest("discover_integration_scope aws.waf integration scope"):
            s3_object_key = "AWSLogs/account-id/WAFLogs/Region/web-acl-name/YYYY/MM/dd/HH/mm"

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.waf"

        with self.subTest("discover_integration_scope aws.vpcflow integration scope"):
            s3_object_key = "AWSLogs/id/vpcflowlogs/region/date_vpcflowlogs_region_file.log.gz"

            assert discover_integration_scope(s3_object_key=s3_object_key) == "aws.vpcflow"

        with self.subTest("discover_integration_scope unknown integration scope"):
            s3_object_key = "random_hash"

            assert discover_integration_scope(s3_object_key=s3_object_key) == "generic"

        with self.subTest("discover_integration_scope empty s3"):
            s3_object_key = ""

            assert discover_integration_scope(s3_object_key=s3_object_key) == "generic"


@pytest.mark.unit
class TestGetShipperFromInput(TestCase):
    def test_get_shipper_from_input(self) -> None:
        with self.subTest("Logstash shipper from Kinesis input"):
            config_yaml_kinesis: str = """
                                inputs:
                                  - type: kinesis-data-stream
                                    id: arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            config = parse_config(config_yaml_kinesis)
            event_input = config.get_input_by_id(
                "arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream"
            )
            assert event_input is not None
            shipper = get_shipper_from_input(event_input=event_input)
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)
            event_input.delete_output_by_destination("logstash_url")

        with self.subTest("Logstash shipper from Cloudwatch logs input"):
            config_yaml_cw: str = """
                                inputs:
                                  - type: cloudwatch-logs
                                    id: arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            config = parse_config(config_yaml_cw)
            event_input = config.get_input_by_id("arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs")
            assert event_input is not None
            shipper = get_shipper_from_input(event_input=event_input)
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)

        with self.subTest("Logstash shipper from each input"):
            config_yaml_cw = """
                                inputs:
                                  - type: cloudwatch-logs
                                    id: arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                                  - type: kinesis-data-stream
                                    id: arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            config = parse_config(config_yaml_cw)
            event_input_cw = config.get_input_by_id("arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs")
            assert event_input_cw is not None
            shipper = get_shipper_from_input(event_input=event_input_cw)
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)

            event_input_kinesis = config.get_input_by_id(
                "arn:aws:kinesis:eu-central-1:123456789:stream/test-esf" "-kinesis-stream"
            )
            assert event_input_kinesis is not None
            shipper = get_shipper_from_input(event_input=event_input_kinesis)
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)

            event_input_cw.delete_output_by_destination("logstash_url")
            event_input_kinesis.delete_output_by_destination("logstash_url")

        with self.subTest("Two Logstash shippers from Cloudwatch logs input"):
            config_yaml_cw = """
                                inputs:
                                  - type: cloudwatch-logs
                                    id: arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url-1
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url-2
                            """
            config = parse_config(config_yaml_cw)
            event_input = config.get_input_by_id("arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs")
            assert event_input is not None
            shipper = get_shipper_from_input(event_input=event_input)
            assert len(shipper._shippers) == 2
            assert isinstance(shipper._shippers[0], LogstashShipper)
            assert isinstance(shipper._shippers[1], LogstashShipper)
            event_input.delete_output_by_destination("logstash_url-1")
            event_input.delete_output_by_destination("logstash_url-2")

        with self.subTest("Two outputs with the same logstash_url"):
            config_yaml_cw = """
                                inputs:
                                  - type: cloudwatch-logs
                                    id: arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            with self.assertRaisesRegex(ValueError, "logstash_url"):
                parse_config(config_yaml_cw)


@pytest.mark.unit
class TestRecordId(TestCase):
    def test_kinesis_id_less_than_512bytes(self) -> None:
        stream_name: str = _get_random_string_of_size(MAX_STREAM_NAME_CHARS)
        partition_key: str = _get_random_string_of_size(MAX_PARTITION_KEY_CHARS)
        sequence_number: str = _get_random_digit_string_of_size(MAX_SEQUENCE_NUMBER_DIGITS)
        approximate_arrival_timestamp: int = int(datetime.utcnow().timestamp() * 1000)
        relevant_fields_for_id: dict[str, Any] = {
            "fields": {
                "log": {"offset": 1},
                "aws": {
                    "kinesis": {
                        "type": "stream",
                        "name": stream_name,
                        "partition_key": partition_key,
                        "sequence_number": sequence_number,
                    }
                },
            },
            "meta": {
                "approximate_arrival_timestamp": approximate_arrival_timestamp,
            },
        }

        generated_id = kinesis_record_id(relevant_fields_for_id)
        assert _utf8len(generated_id) <= MAX_ES_ID_SIZ_BYTES

    def test_s3_id_less_than_512bytes(self) -> None:
        event_time: int = int(datetime.utcnow().timestamp() * 1000)
        bucket_name: str = _get_random_string_of_size(MAX_BUCKET_NAME_CHARS)
        bucket_arn: str = f"arn:aws:s3:::{bucket_name}"
        object_key: str = _get_random_string_of_size(MAX_OBJECT_KEY_CHARS)
        relevant_fields_for_id: dict[str, Any] = {
            "fields": {
                "log": {
                    "offset": 1,
                },
                "aws": {
                    "s3": {
                        "bucket": {"arn": bucket_arn},
                        "object": {"key": object_key},
                    }
                },
            },
            "meta": {"event_time": event_time},
        }
        generated_id = s3_object_id(relevant_fields_for_id)
        assert _utf8len(generated_id) <= MAX_ES_ID_SIZ_BYTES

    def test_sqs_id_less_than_512bytes(self) -> None:
        sent_timestamp: int = int(datetime.utcnow().timestamp() * 1000)
        queue_name: str = _get_random_string_of_size(MAX_QUEUE_NAME_CHARS)
        message_id: str = _get_random_string_of_size(MAX_MESSAGE_ID_CHARS)

        relevant_fields_for_id: dict[str, Any] = {
            "fields": {
                "log": {
                    "offset": 1,
                },
                "aws": {
                    "sqs": {
                        "name": queue_name,
                        "message_id": message_id,
                    },
                },
            },
            "meta": {"sent_timestamp": sent_timestamp},
        }

        generated_id = sqs_object_id(relevant_fields_for_id)
        assert _utf8len(generated_id) <= MAX_ES_ID_SIZ_BYTES

    def test_cloudwatch_id_less_than_512bytes(self) -> None:
        event_timestamp: int = int(datetime.utcnow().timestamp() * 1000)
        log_group_name: str = _get_random_string_of_size(MAX_CW_LOG_GROUP_NAME_CHARS)
        log_stream_name: str = _get_random_string_of_size(MAX_CW_LOG_STREAM_NAME_CHARS)
        event_id: str = _get_random_string_of_size(MAX_CW_EVENT_ID_CHARS)

        relevant_fields_for_id: dict[str, Any] = {
            "fields": {
                "log": {
                    "offset": 1,
                },
                "aws": {
                    "cloudwatch": {
                        "log_group": log_group_name,
                        "log_stream": log_stream_name,
                        "event_id": event_id,
                    }
                },
            },
            "meta": {"event_timestamp": event_timestamp},
        }

        generated_id = cloudwatch_logs_object_id(relevant_fields_for_id)
        assert _utf8len(generated_id) <= MAX_ES_ID_SIZ_BYTES
