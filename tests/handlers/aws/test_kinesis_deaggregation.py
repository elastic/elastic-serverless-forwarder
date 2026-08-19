# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import gzip
import hashlib
import json
from typing import Any, Optional

import aws_kinesis_agg
import pytest
from aws_kinesis_agg.aggregator import RecordAggregator

from handlers.aws.kinesis_deaggregation import deaggregate_kinesis_records

_KPL_MAGIC: bytes = aws_kinesis_agg.MAGIC


def _lambda_kinesis_record(data: bytes, partition_key: str = "partition-key") -> dict[str, Any]:
    """Builds a kinesis record shaped the way AWS Lambda delivers it."""
    return {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": partition_key,
            "sequenceNumber": "49568167373333333333333333333333333333333333333333333333",
            "data": base64.b64encode(data).decode("utf-8"),
            "approximateArrivalTimestamp": 1618434587.036,
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": "shardId-000000000000:49568167373333333333333333333333333333333333333333333333",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
        "awsRegion": "eu-central-1",
        "eventSourceARN": "arn:aws:kinesis:eu-central-1:123456789012:stream/test-esf-kinesis-stream",
    }


def _aggregate(payloads: list[bytes], partition_keys: Optional[list[str]] = None) -> bytes:
    """Builds a real KPL aggregated record using the reference implementation."""
    aggregator = RecordAggregator()
    for n, payload in enumerate(payloads):
        partition_key = partition_keys[n] if partition_keys is not None else f"partition-key-{n}"
        aggregator.add_user_record(partition_key, payload)

    _, _, data = aggregator.clear_and_get().get_contents()
    assert isinstance(data, bytes)
    return data


def _aggregated_record(message: bytes) -> bytes:
    """Frames a protobuf message as an aggregated record, digest included, so it is recognised."""
    return _KPL_MAGIC + message + hashlib.md5(message).digest()


def _length_delimited_field(field_number: int, value: bytes) -> bytes:
    """Encodes a length delimited protobuf field. Only lengths below 128 are needed here."""
    assert len(value) < 128
    return bytes([field_number << 3 | 2, len(value)]) + value


def _varint_field(field_number: int, value: int) -> bytes:
    """Encodes a varint protobuf field. Only values below 128 are needed here."""
    assert value < 128
    return bytes([field_number << 3, value])


def _access_log_payload(n: int) -> bytes:
    """A payload shaped and sized like the access log lines these streams actually carry."""
    return json.dumps(
        {
            "account_id": f"account-{n}",
            "client_ip": f"203.0.113.{n % 256}",
            "domain_name": f"host-{n}.example.invalid",
            "event_timestamp": "2026-08-19T00:00:00Z",
            "first_byte_delay": "0.001",
            "referer": f"https://host-{n}.example.invalid/referer/path/{n}",
            "request": f"GET /some/reasonably/long/request/path/{n}?query=value&other=value HTTP/1.1",
            "request_bytes": "512",
            "request_method": "GET",
            "request_scheme": "https",
            "response_bytes": "2048",
            "response_code": "200",
            "server": f"server-{n}",
            "server_ip": f"198.51.100.{n % 256}",
            "server_response_time": "0.010",
            "user_agent": "Mozilla/5.0 (compatible; ExampleBot/1.0; +https://example.invalid/bot)",
            "x_powered_by": "PHP/8.2.0",
        }
    ).encode("utf-8")


def _readable_message() -> bytes:
    """A protobuf message that on its own resolves to exactly one user record."""
    user_record = _varint_field(1, 0) + _length_delimited_field(3, b'{"msg":"one"}')

    return _length_delimited_field(1, b"partition-key-0") + _length_delimited_field(3, user_record)


def _aggregate_with_unresolvable_partition_key() -> bytes:
    """
    Builds an aggregated record whose digest is valid but whose partition key index points past the
    partition key table, so its user records cannot be resolved.
    """
    # imported here because the module is only importable once aws_kinesis_agg has fixed up sys.path
    from aws_kinesis_agg import messages_pb2

    aggregated_record = messages_pb2.AggregatedRecord()
    aggregated_record.partition_key_table.append("partition-key-0")
    user_record = aggregated_record.records.add()
    user_record.partition_key_index = 5
    user_record.data = b'{"msg":"one"}'

    message = aggregated_record.SerializeToString()
    assert isinstance(message, bytes)
    return _aggregated_record(message)


@pytest.mark.unit
class TestDeaggregateKinesisRecords:
    def test_aggregated_record_expands_into_its_user_records(self) -> None:
        aggregated = _aggregate([b'{"msg":"one"}', b'{"msg":"two"}', b'{"msg":"three"}'])

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert [base64.b64decode(record["kinesis"]["data"]) for record in deaggregated] == [
            b'{"msg":"one"}',
            b'{"msg":"two"}',
            b'{"msg":"three"}',
        ]

    def test_user_record_data_is_a_string_as_lambda_delivers_it(self) -> None:
        aggregated = _aggregate([b'{"msg":"one"}'])

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert isinstance(deaggregated[0]["kinesis"]["data"], str)

    def test_record_the_deaggregator_cannot_expand_is_not_dropped(self) -> None:
        record = _lambda_kinesis_record(_aggregate_with_unresolvable_partition_key())

        deaggregated = deaggregate_kinesis_records([record])

        assert deaggregated == [record]

    def test_deaggregation_failure_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        record = _lambda_kinesis_record(_aggregate_with_unresolvable_partition_key())

        with caplog.at_level("ERROR"):
            deaggregate_kinesis_records([record])

        assert "cannot deaggregate kinesis record" in caplog.text

    def test_record_that_was_not_aggregated_is_returned_untouched(self) -> None:
        record = _lambda_kinesis_record(b'{"msg":"not aggregated"}')

        assert deaggregate_kinesis_records([record]) == [record]

    def test_payload_shorter_than_the_magic_header_is_returned_untouched(self) -> None:
        record = _lambda_kinesis_record(b"ab")

        assert deaggregate_kinesis_records([record]) == [record]

    def test_payload_with_the_magic_header_but_no_digest_is_returned_untouched(self) -> None:
        record = _lambda_kinesis_record(_KPL_MAGIC + b"too short to hold a digest")

        assert deaggregate_kinesis_records([record]) == [record]

    def test_aggregated_record_with_a_corrupted_digest_is_returned_untouched(self) -> None:
        corrupted = bytearray(_aggregate([b'{"msg":"one"}']))
        corrupted[-1] ^= 0xFF
        record = _lambda_kinesis_record(bytes(corrupted))

        assert deaggregate_kinesis_records([record]) == [record]

    def test_user_records_carry_their_sub_sequence_number(self) -> None:
        aggregated = _aggregate([b'{"msg":"one"}', b'{"msg":"two"}', b'{"msg":"three"}'])

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert [record["kinesis"]["subSequenceNumber"] for record in deaggregated] == [0, 1, 2]

    def test_user_records_carry_the_partition_key_they_were_aggregated_with(self) -> None:
        aggregated = _aggregate([b'{"msg":"one"}', b'{"msg":"two"}'], partition_keys=["first", "second"])

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert [record["kinesis"]["partitionKey"] for record in deaggregated] == ["first", "second"]

    def test_user_records_keep_the_metadata_of_the_record_they_came_from(self) -> None:
        record = _lambda_kinesis_record(_aggregate([b'{"msg":"one"}', b'{"msg":"two"}']))

        deaggregated = deaggregate_kinesis_records([record])

        for user_record in deaggregated:
            assert user_record["eventSourceARN"] == record["eventSourceARN"]
            assert user_record["awsRegion"] == record["awsRegion"]
            assert user_record["kinesis"]["sequenceNumber"] == record["kinesis"]["sequenceNumber"]
            assert (
                user_record["kinesis"]["approximateArrivalTimestamp"]
                == record["kinesis"]["approximateArrivalTimestamp"]
            )

    def test_user_records_with_a_binary_payload_are_not_corrupted(self) -> None:
        # fluent-bit can compress what it sends: the payload of a user record is not always text
        gzipped = gzip.compress(b'{"msg":"one"}')
        aggregated = _aggregate([gzipped])

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert base64.b64decode(deaggregated[0]["kinesis"]["data"]) == gzipped

    def test_aggregated_and_plain_records_in_the_same_batch_are_both_handled(self) -> None:
        aggregated = _lambda_kinesis_record(_aggregate([b'{"msg":"one"}', b'{"msg":"two"}']))
        plain = _lambda_kinesis_record(b'{"msg":"three"}')

        deaggregated = deaggregate_kinesis_records([aggregated, plain])

        assert [base64.b64decode(record["kinesis"]["data"]) for record in deaggregated] == [
            b'{"msg":"one"}',
            b'{"msg":"two"}',
            b'{"msg":"three"}',
        ]

    def test_empty_batch_is_handled(self) -> None:
        assert deaggregate_kinesis_records([]) == []

    def test_aggregated_record_built_by_the_reference_implementation_is_expanded(self) -> None:
        # captured from awslabs/kinesis-aggregation, so the format we accept is not defined by the
        # same code that produces the aggregated records in the tests above
        reference_aggregated_record = (
            "84mawgoEcGstMAoEcGstMQoEcGstMhoTCAAaD3sibXNnIjoibGluZTAifRoTCAEaD3sibXNnIjoi"
            "bGluZTEifRoTCAIaD3sibXNnIjoibGluZTIifZ8hyadI+v2KZPZrUGim6cQ="
        )

        deaggregated = deaggregate_kinesis_records(
            [_lambda_kinesis_record(base64.b64decode(reference_aggregated_record))]
        )

        assert [
            (record["kinesis"]["partitionKey"], base64.b64decode(record["kinesis"]["data"])) for record in deaggregated
        ] == [
            ("pk-0", b'{"msg":"line0"}'),
            ("pk-1", b'{"msg":"line1"}'),
            ("pk-2", b'{"msg":"line2"}'),
        ]

    def test_aggregated_record_carrying_no_user_record_is_not_dropped(self) -> None:
        message = _length_delimited_field(1, b"partition-key-0")
        record = _lambda_kinesis_record(_aggregated_record(message))

        assert deaggregate_kinesis_records([record]) == [record]

    def test_aggregated_record_with_a_field_overrunning_the_message_is_not_dropped(self) -> None:
        # a length delimited field announcing more bytes than the message holds. It trails a user
        # record that reads correctly, so the record is forwarded because of the damaged field and
        # not merely because nothing could be read out of the message.
        record = _lambda_kinesis_record(_aggregated_record(_readable_message() + bytes([1 << 3 | 2, 127])))

        assert deaggregate_kinesis_records([record]) == [record]

    def test_aggregated_record_with_an_unsupported_wire_type_is_not_dropped(self) -> None:
        # wire type 3 belongs to the group encoding, which the schema does not use
        record = _lambda_kinesis_record(_aggregated_record(_readable_message() + bytes([1 << 3 | 3])))

        assert deaggregate_kinesis_records([record]) == [record]

    def test_aggregated_record_with_an_unterminated_varint_is_not_dropped(self) -> None:
        # every byte has its continuation bit set, so the varint never ends
        record = _lambda_kinesis_record(_aggregated_record(_readable_message() + bytes([0x80] * 4)))

        assert deaggregate_kinesis_records([record]) == [record]

    def test_user_records_are_recovered_when_an_explicit_hash_key_table_is_present(self) -> None:
        # the explicit hash key table is a field of the schema this reader has no use for
        aggregator = RecordAggregator()
        aggregator.add_user_record("partition-key-0", b'{"msg":"one"}', explicit_hash_key="0")
        aggregator.add_user_record("partition-key-1", b'{"msg":"two"}', explicit_hash_key="1")
        _, _, aggregated = aggregator.clear_and_get().get_contents()

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(aggregated)])

        assert [base64.b64decode(record["kinesis"]["data"]) for record in deaggregated] == [
            b'{"msg":"one"}',
            b'{"msg":"two"}',
        ]

    def test_user_records_are_recovered_whatever_order_the_fields_are_serialised_in(self) -> None:
        # a partition key index has to be resolved against the complete table, so the user records
        # cannot be built before every field of the message has been read
        user_record = _varint_field(1, 1) + _length_delimited_field(3, b'{"msg":"one"}')
        message = (
            _length_delimited_field(3, user_record)
            + _length_delimited_field(1, b"partition-key-0")
            + _length_delimited_field(1, b"partition-key-1")
        )

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(_aggregated_record(message))])

        assert len(deaggregated) == 1
        assert deaggregated[0]["kinesis"]["partitionKey"] == "partition-key-1"
        assert base64.b64decode(deaggregated[0]["kinesis"]["data"]) == b'{"msg":"one"}'

    def test_user_records_longer_than_a_one_byte_length_are_recovered(self) -> None:
        # an access log line runs to several hundred bytes, so its length in the protobuf message
        # does not fit in a single varint byte the way the short payloads above do
        payloads = [_access_log_payload(n) for n in range(80)]
        assert min(len(payload) for payload in payloads) > 127

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(_aggregate(payloads))])

        assert [base64.b64decode(record["kinesis"]["data"]) for record in deaggregated] == payloads

    def test_user_record_longer_than_a_two_byte_length_is_recovered(self) -> None:
        # a length above 16383 needs a third varint byte
        payload = b'{"msg":"' + b"x" * 20000 + b'"}'

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(_aggregate([payload]))])

        assert base64.b64decode(deaggregated[0]["kinesis"]["data"]) == payload

    def test_user_records_that_can_be_read_survive_one_that_cannot(self) -> None:
        # one user record pointing outside the partition key table must not cost the aggregated
        # record every other user record it carries
        readable = _varint_field(1, 0) + _length_delimited_field(3, b'{"msg":"one"}')
        unreadable = _varint_field(1, 5) + _length_delimited_field(3, b'{"msg":"two"}')
        message = (
            _length_delimited_field(1, b"partition-key-0")
            + _length_delimited_field(3, readable)
            + _length_delimited_field(3, unreadable)
        )

        deaggregated = deaggregate_kinesis_records([_lambda_kinesis_record(_aggregated_record(message))])

        assert [base64.b64decode(record["kinesis"]["data"]) for record in deaggregated] == [b'{"msg":"one"}']

    def test_unreadable_user_records_are_reported_once_for_the_whole_record(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # one report per kinesis record, however many of its user records could not be read
        readable = _varint_field(1, 0) + _length_delimited_field(3, b'{"msg":"one"}')
        unreadable = _varint_field(1, 5) + _length_delimited_field(3, b'{"msg":"two"}')
        message = (
            _length_delimited_field(1, b"partition-key-0")
            + _length_delimited_field(3, readable)
            + _length_delimited_field(3, unreadable) * 3
        )

        with caplog.at_level("ERROR"):
            deaggregate_kinesis_records([_lambda_kinesis_record(_aggregated_record(message))])

        assert caplog.text.count("cannot read some user records") == 1
