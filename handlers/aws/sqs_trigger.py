# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import extract_events_from_field, shared_logger
from storage import CommonStorage, StorageFactory

from .event import _default_event
from .utils import (
    extractor_events_from_field,
    get_account_id_from_arn,
    get_queue_url_from_sqs_arn,
    get_sqs_queue_name_and_region_from_arn,
)


def _handle_sqs_continuation(
    sqs_client: BotoBaseClient,
    sqs_continuing_queue: str,
    last_ending_offset: Optional[int],
    sqs_record: dict[str, Any],
    event_input_id: str,
    config_yaml: str,
) -> None:
    """
    Handler of the continuation queue for sqs inputs
    If a sqs message cannot be fully processed before the
    timeout of the lambda this handler will be called: it will
    send new sqs messages for the unprocessed records to the
    internal continuing sqs queue
    """

    message_attributes = {}
    if "messageAttributes" in sqs_record:
        for attribute in sqs_record["messageAttributes"]:
            new_attribute = {}
            for attribute_key in sqs_record["messageAttributes"][attribute]:
                camel_case_key = "".join([attribute_key[0].upper(), attribute_key[1:]])
                new_attribute[camel_case_key] = sqs_record["messageAttributes"][attribute][attribute_key]

            message_attributes[attribute] = new_attribute
    else:
        message_attributes = {
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalMessageId": {"StringValue": sqs_record["messageId"], "DataType": "String"},
            "originalEventSourceARN": {"StringValue": event_input_id, "DataType": "String"},
        }

    if last_ending_offset is not None:
        message_attributes["originalLastEndingOffset"] = {"StringValue": str(last_ending_offset), "DataType": "Number"}

    sqs_client.send_message(
        QueueUrl=sqs_continuing_queue,
        MessageBody=sqs_record["body"],
        MessageAttributes=message_attributes,
    )

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
    sqs_record: dict[str, Any], is_continuation_of_cloudwatch_logs: bool, input_id: str, integration_scope: str
) -> Iterator[tuple[dict[str, Any], int, bool]]:
    """
    Handler for sqs inputs.
    It iterates through sqs records in the sqs trigger and process
    content of body payload in the record.
    """

    account_id = get_account_id_from_arn(input_id)

    queue_name, aws_region = get_sqs_queue_name_and_region_from_arn(input_id)
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

    for log_event, json_object, ending_offset, starting_offset, newline_length in events:
        assert isinstance(log_event, bytes)

        for extracted_log_event, extracted_starting_offset, is_last_event_extracted in extract_events_from_field(
            log_event, json_object, starting_offset, ending_offset, integration_scope, extractor_events_from_field
        ):
            es_event = deepcopy(_default_event)
            es_event["@timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            es_event["fields"]["message"] = extracted_log_event.decode("UTF-8")

            es_event["fields"]["log"]["offset"] = extracted_starting_offset

            if is_continuation_of_cloudwatch_logs:
                assert "originalEventId" in payload
                event_id = payload["originalEventId"]["stringValue"]

                assert "originalLogGroup" in payload
                log_group_name = payload["originalLogGroup"]["stringValue"]

                assert "originalLogStream" in payload
                log_stream_name = payload["originalLogStream"]["stringValue"]

                es_event["fields"]["log"]["file"]["path"] = f"{log_group_name}/{log_stream_name}"

                es_event["fields"]["aws"] = {
                    "cloudwatch": {
                        "log_group": log_group_name,
                        "log_stream": log_stream_name,
                        "event_id": event_id,
                    }
                }
            else:
                es_event["fields"]["log"]["file"]["path"] = get_queue_url_from_sqs_arn(input_id)

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
            es_event["fields"]["cloud"]["account"] = {"id": account_id}

            yield es_event, ending_offset, is_last_event_extracted
