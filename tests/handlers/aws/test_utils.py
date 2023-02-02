# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import random
import string
from datetime import datetime
from typing import Any
from unittest import TestCase

import pytest

from handlers.aws.utils import cloudwatch_logs_object_id, kinesis_record_id, s3_object_id, sqs_object_id

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
class TestUtils(TestCase):
    def test_kinesis_id_less_than_512bytes(self) -> None:
        stream_name: str = _get_random_string_of_size(MAX_STREAM_NAME_CHARS)
        partition_key: str = _get_random_string_of_size(MAX_PARTITION_KEY_CHARS)
        sequence_number: str = _get_random_digit_string_of_size(MAX_SEQUENCE_NUMBER_DIGITS)
        approximate_arrival_timestamp: int = int(datetime.utcnow().timestamp())
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
        event_time: int = int(datetime.utcnow().timestamp())
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
        sent_timestamp: int = int(datetime.utcnow().timestamp())
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
        event_timestamp: int = int(datetime.utcnow().timestamp())
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
