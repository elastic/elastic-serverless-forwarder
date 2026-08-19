# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
from typing import Any, Optional

import mock
import pytest

from handlers.aws.kinesis_trigger import _handle_kinesis_move, _handle_kinesis_record
from handlers.aws.utils import INTEGRATION_SCOPE_GENERIC, expand_event_list_from_field_resolver
from share import ExpandEventListFromField

_INPUT_ID = "arn:aws:kinesis:eu-central-1:123456789012:stream/test-esf-kinesis-stream"


def _kinesis_record(payload: bytes, subsequence_number: Optional[int] = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "partition-key",
            "sequenceNumber": "4956816737333",
            "data": base64.b64encode(payload).decode("utf-8"),
            "approximateArrivalTimestamp": 1618434587.036,
        },
        "eventSourceARN": _INPUT_ID,
        "awsRegion": "eu-central-1",
    }

    if subsequence_number is not None:
        record["kinesis"]["subSequenceNumber"] = subsequence_number

    return record


def _events_for(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expander = ExpandEventListFromField("", INTEGRATION_SCOPE_GENERIC, expand_event_list_from_field_resolver)

    return [
        es_event for es_event, _, _, _ in _handle_kinesis_record({"Records": records}, _INPUT_ID, expander, None, None)
    ]


@pytest.mark.unit
class TestHandleKinesisRecord:
    def test_sub_sequence_number_of_a_user_record_is_kept_in_the_event(self) -> None:
        events = _events_for([_kinesis_record(b'{"msg":"one"}', subsequence_number=7)])

        assert events[0]["fields"]["aws"]["kinesis"]["subsequence_number"] == 7

    def test_record_without_a_sub_sequence_number_does_not_get_one(self) -> None:
        events = _events_for([_kinesis_record(b'{"msg":"one"}')])

        assert "subsequence_number" not in events[0]["fields"]["aws"]["kinesis"]


@pytest.mark.unit
class TestHandleKinesisMove:
    def _sent_message_attributes(self, record: dict[str, Any]) -> dict[str, Any]:
        sqs_client = mock.MagicMock()

        _handle_kinesis_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.eu-central-1.amazonaws.com/123456789012/continuing-queue",
            kinesis_record=record,
            event_input_id=_INPUT_ID,
            config_yaml="inputs: []",
        )

        message_attributes = sqs_client.send_message.call_args.kwargs["MessageAttributes"]
        assert isinstance(message_attributes, dict)
        return message_attributes

    def test_sub_sequence_number_is_forwarded_to_the_continuing_queue(self) -> None:
        message_attributes = self._sent_message_attributes(_kinesis_record(b'{"msg":"one"}', subsequence_number=7))

        assert message_attributes["originalSubsequenceNumber"]["StringValue"] == "7"

    def test_record_without_a_sub_sequence_number_does_not_forward_one(self) -> None:
        message_attributes = self._sent_message_attributes(_kinesis_record(b'{"msg":"one"}'))

        assert "originalSubsequenceNumber" not in message_attributes
