# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

# Function names should be:
#   <service>_<name>
#
# Where service is the boto3 service name.
# If the function does not related to AWS do not add the value and considering
# moving in a file shared across different handlers.

import base64
import datetime
import gzip
import os.path
import random
import string
import time
from copy import deepcopy
from typing import Any, Union

from botocore.client import BaseClient as BotoBaseClient

from share import json_dumper

_AWS_REGION = "us-east-1"

_REMAINING_TIME_FORCE_CONTINUE_0ms = 0

_S3_NOTIFICATION_EVENT_TIME: str = "2021-09-08T18:34:25.042Z"


class ContextMock:
    def __init__(self, remaining_time_in_millis: int = _REMAINING_TIME_FORCE_CONTINUE_0ms):
        self._remaining_time_in_millis = remaining_time_in_millis

    aws_request_id = "aws_request_id"
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789:function:elastic-serverless-forwarder"
    memory_limit_in_mb = "512"
    function_version = "v0.0.0"

    def get_remaining_time_in_millis(self) -> int:
        return self._remaining_time_in_millis


def _load_file_fixture(name: str) -> str:
    """
    Load a file from testdata folder to be used as fixture.
    """
    filepath = os.path.join(os.path.dirname(__file__), "testdata", name)

    res = ""
    with open(filepath) as f:
        res = f.read()

    return res


def _time_based_id(prefix: str = "", suffix: str = "") -> str:
    now = int(datetime.datetime.utcnow().timestamp() * 1000)
    if prefix:
        prefix = f"{prefix}-"
    if suffix:
        suffix = f"-{suffix}"

    return f"{prefix}{now}{suffix}".lower()


def _logs_create_cloudwatch_logs_group(client: BotoBaseClient, group_name: str) -> Any:
    client.create_log_group(logGroupName=group_name)
    return client.describe_log_groups(logGroupNamePrefix=group_name)["logGroups"][0]


def _logs_create_cloudwatch_logs_stream(client: BotoBaseClient, group_name: str, stream_name: str) -> Any:
    client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)

    return client.describe_log_streams(logGroupName=group_name, logStreamNamePrefix=stream_name)["logStreams"][0]


def _logs_upload_event_to_cloudwatch_logs(
    client: BotoBaseClient, group_name: str, stream_name: str, messages_body: list[str]
) -> None:
    now = int(datetime.datetime.utcnow().timestamp() * 1000)
    client.put_log_events(
        logGroupName=group_name,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": now + (n * 1000), "message": message_body} for n, message_body in enumerate(messages_body)
        ],
    )


def _logs_retrieve_event_from_cloudwatch_logs(
    client: BotoBaseClient, group_name: str, stream_name: str
) -> tuple[dict[str, Any], list[str], list[int]]:
    collected_log_event_ids: list[str] = []
    collected_log_event_timestamp: list[int] = []
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
        collected_log_event_timestamp.append(int(float(event["timestamp"])))

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

    data_gzip = gzip.compress(data_json.encode("utf-8"))
    data_base64encoded = base64.b64encode(data_gzip)

    return {"awslogs": {"data": data_base64encoded}}, collected_log_event_ids, collected_log_event_timestamp


def _s3_upload_content_to_bucket(
    client: BotoBaseClient,
    content: bytes,
    key: str,
    bucket_name: str,
    content_type: str,
    create_bucket: bool = True,
    acl: str = "public-read-write",
) -> None:
    if create_bucket:
        client.create_bucket(Bucket=bucket_name, ACL=acl)

    client.put_object(Bucket=bucket_name, Key=key, Body=content, ContentType=content_type, CacheControl="no-cache")


def _kinesis_create_stream(client: BotoBaseClient, stream_name: str) -> Any:
    client.create_stream(StreamName=stream_name, ShardCount=1)
    kinesis_waiter = client.get_waiter("stream_exists")
    while True:
        try:
            kinesis_waiter.wait(StreamName=stream_name)
        except Exception:
            time.sleep(1)
        else:
            break

    return client.describe_stream(StreamName=stream_name)


def _kinesis_put_records(client: BotoBaseClient, stream_name: str, records_data: list[str]) -> None:
    records: list[dict[str, str]] = []
    for data in records_data:
        records.append(
            {
                "PartitionKey": "PartitionKey",
                "Data": base64.b64encode(data.encode("utf-8")).decode("utf-8"),
            }
        )

    client.put_records(Records=records, StreamName=stream_name)


def _kinesis_retrieve_event_from_kinesis_stream(
    client: BotoBaseClient, stream_name: str, stream_arn: str
) -> tuple[dict[str, Any], list[int]]:
    shards_paginator = client.get_paginator("list_shards")
    shards_available = [
        shard
        for shard in shards_paginator.paginate(
            StreamName=stream_name,
            ShardFilter={"Type": "FROM_TRIM_HORIZON", "Timestamp": datetime.datetime(2015, 1, 1)},
            PaginationConfig={"MaxItems": 1, "PageSize": 1},
        )
    ]

    assert len(shards_available) == 1 and len(shards_available[0]["Shards"]) == 1

    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shards_available[0]["Shards"][0]["ShardId"],
        ShardIteratorType="TRIM_HORIZON",
        Timestamp=datetime.datetime(2015, 1, 1),
    )

    records = client.get_records(ShardIterator=shard_iterator["ShardIterator"])

    assert "Records" in records

    new_records: list[dict[str, Any]] = []
    collected_records_approximate_arrival_timestamp: list[int] = []
    for original_record in records["Records"]:
        kinesis_record = {}

        for key in original_record:
            new_value = deepcopy(original_record[key])
            camel_case_key = "".join([key[0].lower(), key[1:]])
            if isinstance(new_value, bytes):
                new_value = new_value.decode("utf-8")

            kinesis_record[camel_case_key] = new_value

        kinesis_record["approximateArrivalTimestamp"] = kinesis_record["approximateArrivalTimestamp"].timestamp()

        new_records.append(
            {
                "kinesis": kinesis_record,
                "eventSource": "aws:kinesis",
                "eventSourceARN": stream_arn,
            }
        )

        collected_records_approximate_arrival_timestamp.append(kinesis_record["approximateArrivalTimestamp"])

    return dict(Records=new_records), collected_records_approximate_arrival_timestamp


def _sqs_create_queue(client: BotoBaseClient, queue_name: str, endpoint_url: str = "") -> dict[str, Any]:
    queue_url = client.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return {
        "QueueArn": queue_arn,
        "QueueUrl": queue_url,
        "QueueUrlPath": queue_url.replace(endpoint_url, f"https://sqs.{_AWS_REGION}.amazonaws.com"),
    }


def _sqs_send_messages(client: BotoBaseClient, queue_url: str, message_body: str) -> None:
    client.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
    )


def _sqs_get_messages(client: BotoBaseClient, queue_url: str, queue_arn: str = "") -> tuple[dict[str, Any], list[str]]:
    """
    A function to extract all messages from a SQS queue, specified by URL.
    """
    collected_messages: list[dict[str, Union[Any, dict[str, Union[str, Any]]]]] = []
    sent_timestamps: list[str] = []
    while True:
        try:
            messages = client.receive_message(QueueUrl=queue_url, AttributeNames=["All"], MessageAttributeNames=["All"])
            # NOTE: asserts are used to check if messages are present.
            # On AssertionError the collected messages are returned.
            assert "Messages" in messages
            assert len(messages["Messages"]) == 1
            original_message = messages["Messages"][0]

            message: dict[str, Any] = {}
            for key in original_message:
                new_value = deepcopy(original_message[key])
                camel_case_key = "".join([key[0].lower(), key[1:]])
                message[camel_case_key] = new_value

            if "messageAttributes" in message:
                for attribute in message["messageAttributes"]:
                    new_attribute = deepcopy(message["messageAttributes"][attribute])
                    for attribute_key in message["messageAttributes"][attribute]:
                        camel_case_key = "".join([attribute_key[0].lower(), attribute_key[1:]])
                        new_attribute[camel_case_key] = new_attribute[attribute_key]
                        new_attribute[attribute_key] = ""

                    message["messageAttributes"][attribute] = new_attribute

            sent_timestamps.append(str(int(message["attributes"]["SentTimestamp"])))

            message["eventSource"] = "aws:sqs"
            message["eventSourceARN"] = queue_arn

            collected_messages.append(message)
        except client.exceptions.OverLimit:
            break
        except AssertionError:
            break

    # sqs is not FIFO, let's make it for tests' sake
    collected_messages.sort(key=lambda x: x["attributes"]["SentTimestamp"])

    return dict(Records=collected_messages), sent_timestamps


def _sqs_get_queue_arn(client: BotoBaseClient, queue_url: str) -> str:
    return str(client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"])


def _create_secrets(client: BotoBaseClient, secret_name: str, secret_data: dict[str, str]) -> Any:
    client.create_secret(Name=secret_name, SecretString=json_dumper(secret_data))
    return client.describe_secret(SecretId=secret_name)["ARN"]


def _sqs_send_s3_notifications(client: BotoBaseClient, queue_url: str, bucket_name: str, filenames: list[str]) -> None:
    records = []
    for filename in filenames:
        records.append(
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "eu-central-1",
                "eventTime": _S3_NOTIFICATION_EVENT_TIME,
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "test-bucket",
                    "bucket": {
                        "name": bucket_name,
                        "arn": f"arn:aws:s3:::{bucket_name}",
                    },
                    "object": {
                        "key": f"{filename}",
                    },
                },
            }
        )

    client.send_message(
        QueueUrl=queue_url,
        MessageBody=json_dumper({"Records": records}),
    )
