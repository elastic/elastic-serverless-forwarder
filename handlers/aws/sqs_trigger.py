# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, shared_logger
from storage import ProtocolStorage, StorageFactory

from .utils import get_account_id_from_arn, get_queue_url_from_sqs_arn, get_sqs_queue_name_and_region_from_arn


def handle_sqs_move(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    sqs_record: dict[str, Any],
    input_id: str,
    config_yaml: str,
    continuing_queue: bool = True,
    last_ending_offset: Optional[int] = None,
    last_event_expanded_offset: Optional[int] = None,
) -> None:
    """
    Handler of the continuation/replay queue for sqs inputs.
    If a sqs message cannot be fully processed before the timeout of the lambda, the handler will be called
    for the continuation queue: it will send new sqs messages for the unprocessed records to the
    internal continuing sqs queue.
    If a sqs message has an eventSourceARN not present in the config.yaml ids, then the handler should be called,
    so it can get placed in the internal replay queue.

    :param continuing_queue: should be set to true if the sqs message is going to be placed in the continuing
    queue. Otherwise, we assume it will be placed in the replaying queue, and, in that case, it should be set to false.
    """

    message_attributes = {}
    if "messageAttributes" in sqs_record:
        for attribute in sqs_record["messageAttributes"]:
            new_attribute = {}
            for attribute_key in sqs_record["messageAttributes"][attribute]:
                if sqs_record["messageAttributes"][attribute][attribute_key]:
                    camel_case_key = "".join([attribute_key[0].upper(), attribute_key[1:]])
                    new_attribute[camel_case_key] = sqs_record["messageAttributes"][attribute][attribute_key]

            message_attributes[attribute] = new_attribute
    else:
        message_attributes = {
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalMessageId": {"StringValue": sqs_record["messageId"], "DataType": "String"},
            "originalSentTimestamp": {
                "StringValue": str(sqs_record["attributes"]["SentTimestamp"]),
                "DataType": "Number",
            },
            "originalEventSourceARN": {"StringValue": input_id, "DataType": "String"},
        }

    if last_ending_offset is not None:
        message_attributes["originalLastEndingOffset"] = {"StringValue": str(last_ending_offset), "DataType": "Number"}

    if last_event_expanded_offset is not None:
        message_attributes["originalLastEventExpandedOffset"] = {
            "StringValue": str(last_event_expanded_offset),
            "DataType": "Number",
        }

    sqs_client.send_message(
        QueueUrl=sqs_destination_queue,
        MessageBody=sqs_record["body"],
        MessageAttributes=message_attributes,
    )

    if continuing_queue:
        shared_logger.debug(
            "continuing",
            extra={
                "sqs_continuing_queue": sqs_destination_queue,
                "last_ending_offset": last_ending_offset,
                "last_event_expanded_offset": last_event_expanded_offset,
                "message_id": sqs_record["messageId"],
            },
        )
    else:
        shared_logger.debug(
            "replaying",
            extra={
                "sqs_replaying_queue": sqs_destination_queue,
                "input_id": input_id,
                "message_id": sqs_record["messageId"],
            },
        )


def _handle_sqs_event(
    sqs_record: dict[str, Any],
    input_id: str,
    event_list_from_field_expander: ExpandEventListFromField,
    continuing_original_input_type: Optional[str],
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[dict[str, Any], int, Optional[int]]]:
    """
    Handler for sqs inputs.
    It iterates through sqs records in the sqs trigger and process
    content of body payload in the record.
    """
    account_id = get_account_id_from_arn(input_id)

    queue_name, aws_region = get_sqs_queue_name_and_region_from_arn(input_id)
    storage: ProtocolStorage = StorageFactory.create(
        storage_type="payload",
        payload=sqs_record["body"],
        json_content_type=json_content_type,
        event_list_from_field_expander=event_list_from_field_expander,
        multiline_processor=multiline_processor,
    )

    range_start = 0

    payload: dict[str, Any] = {}
    if "messageAttributes" in sqs_record:
        payload = sqs_record["messageAttributes"]

    if "originalLastEndingOffset" in payload:
        range_start = int(payload["originalLastEndingOffset"]["stringValue"])

    events = storage.get_by_lines(range_start=range_start)

    for log_event, starting_offset, ending_offset, event_expanded_offset in events:
        assert isinstance(log_event, bytes)

        es_event: dict[str, Any] = {
            "@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "fields": {
                "message": log_event.decode("utf-8"),
                "log": {
                    "offset": starting_offset,
                    "file": {"path": ""},
                },
                "aws": {},
                "cloud": {
                    "provider": "aws",
                    "region": aws_region,
                    "account": {"id": account_id},
                },
            },
            "meta": {},
        }

        if continuing_original_input_type is None:
            es_event["fields"]["log"]["file"]["path"] = get_queue_url_from_sqs_arn(input_id)

            message_id = sqs_record["messageId"]

            if "originalMessageId" in payload:
                message_id = payload["originalMessageId"]["stringValue"]

            if "originalSentTimestamp" in payload:
                sent_timestamp = int(payload["originalSentTimestamp"]["stringValue"])
            else:
                sent_timestamp = sqs_record["attributes"]["SentTimestamp"]

            es_event["fields"]["aws"] = {
                "sqs": {
                    "name": queue_name,
                    "message_id": message_id,
                }
            }

            es_event["meta"]["sent_timestamp"] = sent_timestamp
        elif continuing_original_input_type == "cloudwatch-logs":
            assert "originalEventId" in payload
            event_id = payload["originalEventId"]["stringValue"]

            assert "originalLogGroup" in payload
            log_group_name = payload["originalLogGroup"]["stringValue"]

            assert "originalLogStream" in payload
            log_stream_name = payload["originalLogStream"]["stringValue"]

            assert "originalEventTimestamp" in payload
            event_timestamp = int(float(payload["originalEventTimestamp"]["stringValue"]))

            es_event["fields"]["log"]["file"]["path"] = f"{log_group_name}/{log_stream_name}"

            es_event["fields"]["aws"] = {
                "cloudwatch": {
                    "log_group": log_group_name,
                    "log_stream": log_stream_name,
                    "event_id": event_id,
                }
            }

            es_event["meta"]["event_timestamp"] = event_timestamp
        else:
            assert "originalStreamType" in payload
            stream_type = payload["originalStreamType"]["stringValue"]

            assert "originalStreamName" in payload
            stream_name = payload["originalStreamName"]["stringValue"]

            assert "originalPartitionKey" in payload
            partition_key = payload["originalPartitionKey"]["stringValue"]

            assert "originalSequenceNumber" in payload
            sequence_number = payload["originalSequenceNumber"]["stringValue"]

            assert "originalApproximateArrivalTimestamp" in payload
            approximate_arrival_timestamp = int(
                float(payload["originalApproximateArrivalTimestamp"]["stringValue"]) * 1000
            )

            es_event["fields"]["log"]["file"]["path"] = input_id

            es_event["fields"]["aws"] = {
                "kinesis": {
                    "type": stream_type,
                    "name": stream_name,
                    "partition_key": partition_key,
                    "sequence_number": sequence_number,
                }
            }
            es_event["meta"]["approximate_arrival_timestamp"] = approximate_arrival_timestamp

        yield es_event, ending_offset, event_expanded_offset
