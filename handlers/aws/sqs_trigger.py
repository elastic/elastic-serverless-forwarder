# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any, Iterator

from share import Config, shared_logger
from storage import CommonStorage, StorageFactory

from .event import _default_event
from .utils import delete_sqs_record, get_queue_url_from_sqs_arn, get_sqs_client, get_sqs_queue_name_and_region_from_arn


def _handle_sqs_continuation(
    trigger_event_source_arn: str,
    sqs_continuing_queue: str,
    last_ending_offset: int,
    lambda_event: dict[str, Any],
    event_input_id: str,
    current_sqs_record: int,
    config_yaml: str,
) -> None:
    """
    Handler of the continuation queue for sqs inputs
    If a sqs message cannot be fully processed before the
    timeout of the lambda this handler will be called: it will
    send new sqs messages for the unprocessed records to the
    internal continuing sqs queue
    """

    sqs_records = lambda_event["Records"][current_sqs_record:]

    sqs_client = get_sqs_client()

    for sqs_record in sqs_records:
        sqs_client.send_message(
            QueueUrl=sqs_continuing_queue,
            MessageBody=sqs_record["body"],
            MessageAttributes={
                "config": {"StringValue": config_yaml, "DataType": "String"},
                "originalMessageId": {"StringValue": sqs_record["messageId"], "DataType": "String"},
                "originalEventSource": {"StringValue": event_input_id, "DataType": "String"},
                "originalLastEndingOffset": {"StringValue": str(last_ending_offset), "DataType": "Number"},
            },
        )

        delete_sqs_record(trigger_event_source_arn, sqs_record["receiptHandle"])

        shared_logger.debug(
            "continuing",
            extra={
                "sqs_continuing_queue": sqs_continuing_queue,
                "body": sqs_record["body"],
                "last_ending_offset": last_ending_offset,
                "message_id": sqs_record["messageId"],
            },
        )


def _handle_sqs_event(
    config: Config, event: dict[str, Any], is_continuation_of_cloudwatch_logs: bool
) -> Iterator[tuple[dict[str, Any], int, int]]:
    """
    Handler for sqs inputs.
    It iterates through sqs records in the sqs trigger and process
    content of body payload in the record.
    If a sqs message cannot be fully processed before the
    timeout of the lambda it will call the sqs continuing handler
    """

    for sqs_record_n, sqs_record in enumerate(event["Records"]):
        input_type = "sqs"
        if is_continuation_of_cloudwatch_logs:
            input_type = "cloudwatch-logs"
        event_input = config.get_input_by_type_and_id(input_type, sqs_record["eventSourceARN"])
        if not event_input:
            return None

        queue_name, aws_region = get_sqs_queue_name_and_region_from_arn(sqs_record["eventSourceARN"])
        storage: CommonStorage = StorageFactory.create(storage_type="payload", payload=sqs_record["body"])

        range_start = 0

        payload: dict[str, Any] = {}
        if "messageAttributes" in sqs_record:
            payload = sqs_record["messageAttributes"]

        if "originalLastEndingOffset" in payload:
            range_start = int(payload["originalLastEndingOffset"]["stringValue"])

        events = storage.get_by_lines(
            range_start=range_start,
        )

        for log_event, ending_offset, newline_length in events:
            assert isinstance(log_event, bytes)

            es_event = deepcopy(_default_event)
            es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            es_event["fields"]["message"] = log_event.decode("UTF-8")
            es_event["fields"]["log"]["offset"] = ending_offset - (len(log_event) + newline_length)

            if is_continuation_of_cloudwatch_logs:
                event_id = ""
                log_group_name = ""
                log_stream_name = ""

                if "originalEventId" in payload:
                    event_id = payload["originalEventId"]["stringValue"]

                if "originalLogGroup" in payload:
                    log_group_name = payload["originalLogGroup"]["stringValue"]

                if "originalLogStream" in payload:
                    log_stream_name = payload["originalLogStream"]["stringValue"]

                es_event["fields"]["log"]["file"]["path"] = f"{log_group_name}/{log_stream_name}"

                es_event["fields"]["aws"] = {
                    "cloudwatch_logs": {
                        "group_name": log_group_name,
                        "stream_name": log_stream_name,
                        "event_id": event_id,
                    }
                }
            else:
                es_event["fields"]["log"]["file"]["path"] = get_queue_url_from_sqs_arn(sqs_record["eventSourceARN"])

                message_id = sqs_record["messageId"]

                if "originalMessageId" in payload:
                    message_id = payload["originalMessageId"]["stringValue"]

                es_event["fields"]["aws"] = {
                    "sqs": {
                        "name": queue_name,
                        "message_id": message_id,
                    }
                }

            es_event["fields"]["cloud"]["region"] = aws_region

            yield es_event, ending_offset, sqs_record_n
