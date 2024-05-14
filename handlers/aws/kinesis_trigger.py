# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, shared_logger
from storage import ProtocolStorage, StorageFactory

from .utils import get_account_id_from_arn, get_kinesis_stream_name_type_and_region_from_arn


def _handle_kinesis_move(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    kinesis_record: dict[str, Any],
    event_input_id: str,
    config_yaml: str,
    continuing_queue: bool = True,
    last_ending_offset: Optional[int] = None,
    last_event_expanded_offset: Optional[int] = None,
) -> None:
    """
    Handler of the continuation/replay queue for kinesis data stream inputs.
    If a kinesis data stream records batch cannot be fully processed before the timeout of the lambda, the handler will
    be called for the continuation queue: it will send new sqs messages for the unprocessed records to the
    internal continuing sqs queue.
    If a sqs message has an eventSourceARN not present in the config.yaml ids, then the handler should be called,
    so it can get placed in the internal replay queue.

    :param continuing_queue: should be set to true if the sqs message is going to be placed in the continuing
    queue. Otherwise, we assume it will be placed in the replaying queue, and, in that case, it should be set to false.
    """

    sequence_number = kinesis_record["kinesis"]["sequenceNumber"]
    partition_key = kinesis_record["kinesis"]["partitionKey"]
    approximate_arrival_timestamp = kinesis_record["kinesis"]["approximateArrivalTimestamp"]
    stream_type, stream_name, _ = get_kinesis_stream_name_type_and_region_from_arn(event_input_id)

    message_attributes = {
        "config": {"StringValue": config_yaml, "DataType": "String"},
        "originalStreamType": {"StringValue": stream_type, "DataType": "String"},
        "originalStreamName": {"StringValue": stream_name, "DataType": "String"},
        "originalPartitionKey": {"StringValue": partition_key, "DataType": "String"},
        "originalSequenceNumber": {"StringValue": sequence_number, "DataType": "String"},
        "originalEventSourceARN": {"StringValue": event_input_id, "DataType": "String"},
        "originalApproximateArrivalTimestamp": {
            "StringValue": str(approximate_arrival_timestamp),
            "DataType": "Number",
        },
    }

    if last_ending_offset is not None:
        message_attributes["originalLastEndingOffset"] = {"StringValue": str(last_ending_offset), "DataType": "Number"}

    if last_event_expanded_offset is not None:
        message_attributes["originalLastEventExpandedOffset"] = {
            "StringValue": str(last_event_expanded_offset),
            "DataType": "Number",
        }

    kinesis_data: str = kinesis_record["kinesis"]["data"]

    sqs_client.send_message(
        QueueUrl=sqs_destination_queue,
        MessageBody=kinesis_data,
        MessageAttributes=message_attributes,
    )

    if continuing_queue:
        shared_logger.debug(
            "continuing",
            extra={
                "sqs_continuing_queue": sqs_destination_queue,
                "last_ending_offset": last_ending_offset,
                "last_event_expanded_offset": last_event_expanded_offset,
                "partition_key": partition_key,
                "approximate_arrival_timestamp": approximate_arrival_timestamp,
                "sequence_number": sequence_number,
            },
        )
    else:
        shared_logger.debug(
            "replaying",
            extra={
                "sqs_replaying_queue": sqs_destination_queue,
                "partition_key": partition_key,
                "approximate_arrival_timestamp": approximate_arrival_timestamp,
                "sequence_number": sequence_number,
            },
        )


def _handle_kinesis_record(
    event: dict[str, Any],
    input_id: str,
    event_list_from_field_expander: ExpandEventListFromField,
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[dict[str, Any], int, Optional[int], int]]:
    """
    Handler for kinesis data stream inputs.
    It iterates through kinesis records in the kinesis trigger and process
    the content of kinesis.data payload
    """
    account_id = get_account_id_from_arn(input_id)
    for kinesis_record_n, kinesis_record in enumerate(event["Records"]):
        storage: ProtocolStorage = StorageFactory.create(
            storage_type="payload",
            payload=kinesis_record["kinesis"]["data"],
            json_content_type=json_content_type,
            event_list_from_field_expander=event_list_from_field_expander,
            multiline_processor=multiline_processor,
        )

        stream_type, stream_name, aws_region = get_kinesis_stream_name_type_and_region_from_arn(
            kinesis_record["eventSourceARN"]
        )

        events = storage.get_by_lines(range_start=0)

        for log_event, starting_offset, ending_offset, event_expanded_offset in events:
            assert isinstance(log_event, bytes)

            es_event: dict[str, Any] = {
                "@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "fields": {
                    "message": log_event.decode("utf-8"),
                    "log": {
                        "offset": starting_offset,
                        "file": {
                            "path": kinesis_record["eventSourceARN"],
                        },
                    },
                    "aws": {
                        "kinesis": {
                            "type": stream_type,
                            "name": stream_name,
                            "partition_key": kinesis_record["kinesis"]["partitionKey"],
                            "sequence_number": kinesis_record["kinesis"]["sequenceNumber"],
                        }
                    },
                    "cloud": {
                        "provider": "aws",
                        "region": aws_region,
                        "account": {"id": account_id},
                    },
                },
                "meta": {
                    "approximate_arrival_timestamp": int(
                        float(kinesis_record["kinesis"]["approximateArrivalTimestamp"]) * 1000
                    ),
                },
            }

            yield es_event, ending_offset, event_expanded_offset, kinesis_record_n
