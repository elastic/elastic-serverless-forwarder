# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import json
from copy import deepcopy
from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, shared_logger
from storage import ProtocolStorage, StorageFactory

from .event import _default_event
from .utils import get_account_id_from_arn


def _from_awslogs_data_to_event(awslogs_data: str) -> Any:
    """
    Returns cloudwatch logs event from base64 encoded and gzipped payload
    """
    storage: ProtocolStorage = StorageFactory.create(storage_type="payload", payload=awslogs_data)
    cloudwatch_logs_payload_plain = storage.get_as_string()
    return json.loads(cloudwatch_logs_payload_plain)


def _handle_cloudwatch_logs_continuation(
    sqs_client: BotoBaseClient,
    sqs_continuing_queue: str,
    last_ending_offset: Optional[int],
    last_event_expanded_offset: Optional[int],
    cloudwatch_logs_event: dict[str, Any],
    current_log_event: int,
    event_input_id: str,
    config_yaml: str,
) -> None:
    """
    Handler of the continuation queue for cloudwatch logs inputs
    If a cloudwatch logs data payload cannot be fully processed before the
    timeout of the lambda this handler will be called: it will
    send new sqs messages for the unprocessed payload to the
    internal continuing sqs queue
    """

    log_group_name = cloudwatch_logs_event["logGroup"]
    log_stream_name = cloudwatch_logs_event["logStream"]
    logs_events = cloudwatch_logs_event["logEvents"][current_log_event:]

    for current_log_event, log_event in enumerate(logs_events):
        if current_log_event > 0:
            last_ending_offset = None

        message_attributes = {
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalEventId": {"StringValue": log_event["id"], "DataType": "String"},
            "originalEventSourceARN": {"StringValue": event_input_id, "DataType": "String"},
            "originalLogGroup": {"StringValue": log_group_name, "DataType": "String"},
            "originalLogStream": {"StringValue": log_stream_name, "DataType": "String"},
        }

        if last_ending_offset is not None:
            message_attributes["originalLastEndingOffset"] = {
                "StringValue": str(last_ending_offset),
                "DataType": "Number",
            }

        if last_event_expanded_offset is not None:
            message_attributes["originalLastEventExpandedOffset"] = {
                "StringValue": str(last_event_expanded_offset),
                "DataType": "Number",
            }

        sqs_client.send_message(
            QueueUrl=sqs_continuing_queue,
            MessageBody=log_event["message"],
            MessageAttributes=message_attributes,
        )

        shared_logger.debug(
            "continuing",
            extra={
                "sqs_continuing_queue": sqs_continuing_queue,
                "last_ending_offset": last_ending_offset,
                "last_event_expanded_offset": last_event_expanded_offset,
                "event_id": log_event["id"],
            },
        )


def _handle_cloudwatch_logs_event(
    event: dict[str, Any],
    aws_region: str,
    input_id: str,
    expand_event_list_from_field: ExpandEventListFromField,
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[dict[str, Any], int, Optional[int], int]]:
    """
    Handler for cloudwatch logs inputs.
    It iterates through the logEvents in cloudwatch logs trigger payload and process
    content of body payload in the log event.
    If a log event cannot be fully processed before the
    timeout of the lambda it will call the sqs continuing handler
    """

    account_id = get_account_id_from_arn(input_id)

    log_group_name = event["logGroup"]
    log_stream_name = event["logStream"]

    for cloudwatch_log_event_n, cloudwatch_log_event in enumerate(event["logEvents"]):
        event_id = cloudwatch_log_event["id"]

        storage_message: ProtocolStorage = StorageFactory.create(
            storage_type="payload",
            payload=cloudwatch_log_event["message"],
            json_content_type=json_content_type,
            expand_event_list_from_field=expand_event_list_from_field,
            multiline_processor=multiline_processor,
        )

        events = storage_message.get_by_lines(range_start=0)

        for log_event, starting_offset, ending_offset, event_expanded_offset in events:
            assert isinstance(log_event, bytes)

            es_event = deepcopy(_default_event)
            es_event["@timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            es_event["fields"]["message"] = log_event.decode("UTF-8")

            es_event["fields"]["log"]["offset"] = starting_offset

            es_event["fields"]["log"]["file"]["path"] = f"{log_group_name}/{log_stream_name}"

            es_event["fields"]["aws"] = {
                "cloudwatch": {
                    "log_group": log_group_name,
                    "log_stream": log_stream_name,
                    "event_id": event_id,
                }
            }

            es_event["fields"]["cloud"]["region"] = aws_region
            es_event["fields"]["cloud"]["account"] = {"id": account_id}

            yield es_event, ending_offset, event_expanded_offset, cloudwatch_log_event_n
