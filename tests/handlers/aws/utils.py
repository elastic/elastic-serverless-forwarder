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
from typing import Any, Text

from botocore.client import BaseClient as BotoBaseClient

from share import json_dumper


def _load_file_fixture(name: str) -> str:
    """
    Load a file from testdata folder to be used as fixture.
    """
    filepath = os.path.join(os.path.dirname(__file__), "testdata", name)

    res = ""
    with open(filepath) as f:
        res = f.read()

    return res


def _class_based_id(klass: object, prefix: str = "", suffix: str = "") -> str:
    if prefix:
        prefix = f"{prefix}-"
    if suffix:
        suffix = f"-{suffix}"

    return f"{prefix}{type(klass).__name__}{suffix}"


def _logs_create_cloudwatch_logs_group(client: BotoBaseClient, group_name: str) -> Any:
    client.create_log_group(logGroupName=group_name)
    return client.describe_log_groups(logGroupNamePrefix=group_name)


def _logs_create_cloudwatch_logs_stream(client: BotoBaseClient, group_name: str, stream_name: str) -> Any:
    client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)

    return client.describe_log_streams(logGroupName=group_name, logStreamNamePrefix=stream_name)["logStreams"][0]


def _logs_upload_event_to_cloudwatch_logs(
    client: BotoBaseClient, group_name: str, stream_name: str, messages_body: list[str]
) -> None:
    now = int(datetime.datetime.utcnow().strftime("%s")) * 1000
    client.put_log_events(
        logGroupName=group_name,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": now + (n * 1000), "message": message_body} for n, message_body in enumerate(messages_body)
        ],
    )


def _logs_retrieve_event_from_cloudwatch_logs(
    client: BotoBaseClient, group_name: str, stream_name: str
) -> tuple[dict[str, Any], list[str]]:
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


def _s3_upload_content_to_bucket(
    client: BotoBaseClient, content: Text, key: str, bucket_name: str, content_type: str, acl: str = "public-read-write"
) -> None:
    client.create_bucket(Bucket=bucket_name, ACL=acl)
    client.put_object(Bucket=bucket_name, Key=key, Body=content, ContentType=content_type)


def _sqs_create_queue(client: BotoBaseClient, name: str) -> Any:
    queue = client.create_queue(QueueName=name)
    return queue["QueueUrl"]
