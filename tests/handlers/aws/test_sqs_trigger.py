# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
from typing import Any, Optional

import pytest

from handlers.aws.sqs_trigger import _handle_sqs_event
from handlers.aws.utils import INTEGRATION_SCOPE_GENERIC, expand_event_list_from_field_resolver
from share import ExpandEventListFromField

_KINESIS_INPUT_ID = "arn:aws:kinesis:eu-central-1:123456789012:stream/test-esf-kinesis-stream"


def _continuing_kinesis_sqs_record(payload: bytes, subsequence_number: Optional[int] = None) -> dict[str, Any]:
    """Builds the sqs record the continuing queue holds for a kinesis data stream input."""
    message_attributes: dict[str, Any] = {
        "originalEventSourceARN": {"stringValue": _KINESIS_INPUT_ID, "dataType": "String"},
        "originalStreamType": {"stringValue": "stream", "dataType": "String"},
        "originalStreamName": {"stringValue": "test-esf-kinesis-stream", "dataType": "String"},
        "originalPartitionKey": {"stringValue": "partition-key", "dataType": "String"},
        "originalSequenceNumber": {"stringValue": "4956816737333", "dataType": "String"},
        "originalApproximateArrivalTimestamp": {"stringValue": "1618434587.036", "dataType": "Number"},
    }

    if subsequence_number is not None:
        message_attributes["originalSubsequenceNumber"] = {
            "stringValue": str(subsequence_number),
            "dataType": "Number",
        }

    return {
        "messageId": "message-id",
        "body": base64.b64encode(payload).decode("utf-8"),
        "attributes": {"SentTimestamp": "1618434587036"},
        "messageAttributes": message_attributes,
        "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789012:continuing-queue",
    }


def _events_for(sqs_record: dict[str, Any]) -> list[dict[str, Any]]:
    expander = ExpandEventListFromField("", INTEGRATION_SCOPE_GENERIC, expand_event_list_from_field_resolver)

    return [
        es_event
        for es_event, _, _ in _handle_sqs_event(
            sqs_record, _KINESIS_INPUT_ID, expander, "kinesis-data-stream", None, None
        )
    ]


@pytest.mark.unit
class TestHandleSqsEventContinuingKinesisRecord:
    def test_sub_sequence_number_is_restored_from_the_continuing_queue(self) -> None:
        events = _events_for(_continuing_kinesis_sqs_record(b'{"msg":"one"}', subsequence_number=7))

        assert events[0]["fields"]["aws"]["kinesis"]["subsequence_number"] == 7

    def test_message_without_a_sub_sequence_number_does_not_get_one(self) -> None:
        events = _events_for(_continuing_kinesis_sqs_record(b'{"msg":"one"}'))

        assert "subsequence_number" not in events[0]["fields"]["aws"]["kinesis"]
