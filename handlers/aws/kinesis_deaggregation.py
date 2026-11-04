# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Support for the KPL aggregated record format on kinesis data stream inputs.

A producer with aggregation enabled packs many user records into a single kinesis record:

    [4 bytes magic 0xF3 0x89 0x9A 0xC2][protobuf AggregatedRecord][16 bytes MD5 of the protobuf]

Recovering the user records needs four fields of that schema, which has not changed since the
format was introduced, so the protobuf message is read here rather than by adding a protobuf
runtime to the lambda package. The reader is checked in the tests against aggregated records
built by the reference implementation, awslabs/kinesis-aggregation.
"""

import base64
import hashlib
from typing import Any, Iterator, Optional, Union

from share import shared_logger

_KPL_MAGIC = b"\xf3\x89\x9a\xc2"
_DIGEST_LENGTH = 16

# protobuf wire types
_WIRE_VARINT = 0
_WIRE_64_BIT = 1
_WIRE_LENGTH_DELIMITED = 2
_WIRE_32_BIT = 5

# fields of AggregatedRecord: the explicit hash key table, field 2, is not needed
_FIELD_PARTITION_KEY_TABLE = 1
_FIELD_RECORDS = 3

# fields of AggregatedRecord.Record: the tags, field 4, are not needed
_FIELD_PARTITION_KEY_INDEX = 1
_FIELD_DATA = 3


def deaggregate_kinesis_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Expands every KPL aggregated record in the given kinesis records into the user records it
    carries.

    A record is recognised as aggregated by the KPL magic header and by the digest of its
    payload, so records that were sent without aggregation are returned untouched.

    The returned user records are shaped like the records lambda delivers, each with its own
    partition key and with the sub sequence number it has within its aggregated record.
    """
    deaggregated: list[dict[str, Any]] = []
    for record in records:
        message = _aggregated_message(record["kinesis"]["data"])
        if message is None:
            deaggregated.append(record)
            continue

        try:
            user_records = _user_records(record, message)
        except ValueError as e:
            # the digest matched, so this is an aggregated record we could not read at all.
            # Forward it as it was received: the output gets the frame as opaque text rather than
            # the lines it holds, which at least keeps its bytes together with the error below.
            shared_logger.error(
                "cannot deaggregate kinesis record, forwarding it as it was received",
                extra={"sequence_number": record["kinesis"]["sequenceNumber"], "reason": str(e)},
            )

            user_records = [record]

        deaggregated += user_records

    return deaggregated


def _aggregated_message(data: Union[str, bytes]) -> Optional[bytes]:
    """
    Returns the protobuf message of an aggregated record, or None if the payload does not hold
    one. The digest is checked as well as the magic header: it is what separates an aggregated
    record from a payload that merely begins with the same four bytes.
    """
    try:
        payload = base64.b64decode(data, validate=True)
    except ValueError:
        # both the invalid character error of base64 and its non ascii input error are ValueError
        return None

    if not payload.startswith(_KPL_MAGIC) or len(payload) <= len(_KPL_MAGIC) + _DIGEST_LENGTH:
        return None

    message = payload[len(_KPL_MAGIC) : -_DIGEST_LENGTH]
    if hashlib.md5(message, usedforsecurity=False).digest() != payload[-_DIGEST_LENGTH:]:
        return None

    return message


def _user_records(record: dict[str, Any], message: bytes) -> list[dict[str, Any]]:
    """
    Builds a kinesis record for every user record of the given aggregated record message.

    A user record that cannot be resolved is logged and left out, so that the others are still
    forwarded. ValueError is raised only when the message carries no user record at all, or when
    none of them could be read, as there is nothing left to forward in that case.
    """
    partition_keys: list[str] = []
    record_messages: list[bytes] = []

    # the fields are collected before any of them is resolved, so that a partition key index is
    # looked up in the complete table whatever order the producer serialised the fields in
    for field_number, value in _fields(message):
        if field_number == _FIELD_PARTITION_KEY_TABLE and isinstance(value, bytes):
            partition_keys.append(value.decode("utf-8"))
        elif field_number == _FIELD_RECORDS and isinstance(value, bytes):
            record_messages.append(value)

    if not record_messages:
        raise ValueError("no user record in the aggregated record")

    user_records: list[dict[str, Any]] = []
    unreadable: list[str] = []
    for subsequence_number, record_message in enumerate(record_messages):
        try:
            user_records.append(_user_record(record, record_message, partition_keys, subsequence_number))
        except ValueError as e:
            unreadable.append(str(e))

    # reported once for the whole record rather than once per user record: a producer writing
    # indices it never filled in would otherwise log every user record of every record of a batch
    if unreadable:
        shared_logger.error(
            "cannot read some user records of a kinesis record, skipping them",
            extra={
                "sequence_number": record["kinesis"]["sequenceNumber"],
                "skipped": len(unreadable),
                "user_records": len(record_messages),
                "reason": unreadable[0],
            },
        )

    if not user_records:
        raise ValueError(f"none of the {len(record_messages)} user records could be read")

    return user_records


def _user_record(
    record: dict[str, Any], message: bytes, partition_keys: list[str], subsequence_number: int
) -> dict[str, Any]:
    """Builds a single user record, shaped like the records lambda delivers."""
    partition_key_index = 0
    data = b""

    for field_number, value in _fields(message):
        if field_number == _FIELD_PARTITION_KEY_INDEX and isinstance(value, int):
            partition_key_index = value
        elif field_number == _FIELD_DATA and isinstance(value, bytes):
            data = value

    if partition_key_index >= len(partition_keys):
        raise ValueError(f"partition key {partition_key_index} is not in the partition key table")

    user_record: dict[str, Any] = {key: value for key, value in record.items() if key != "kinesis"}

    # everything the user record shares with the record it was aggregated in is kept, including
    # the sequence number and the arrival timestamp, and the rest is its own
    user_record["kinesis"] = {
        **record["kinesis"],
        "partitionKey": partition_keys[partition_key_index],
        "subSequenceNumber": subsequence_number,
        "data": base64.b64encode(data).decode("utf-8"),
    }

    return user_record


def _fields(message: bytes) -> Iterator[tuple[int, Union[int, bytes]]]:
    """
    Yields the field number and the value of every field of a protobuf message: an int for a
    varint field, the raw bytes for a length delimited one. Fixed width fields are skipped, as
    the schema of an aggregated record has none.
    """
    position = 0
    while position < len(message):
        tag, position = _varint(message, position)
        field_number, wire_type = tag >> 3, tag & 0x07

        if wire_type == _WIRE_VARINT:
            value, position = _varint(message, position)
            yield field_number, value
        elif wire_type == _WIRE_LENGTH_DELIMITED:
            length, position = _varint(message, position)
            end = position + length
            if end > len(message):
                raise ValueError(f"field {field_number} does not fit in the message")

            yield field_number, message[position:end]
            position = end
        elif wire_type in (_WIRE_64_BIT, _WIRE_32_BIT):
            position += 8 if wire_type == _WIRE_64_BIT else 4
            if position > len(message):
                raise ValueError(f"field {field_number} does not fit in the message")
        else:
            raise ValueError(f"field {field_number} has unsupported wire type {wire_type}")


def _varint(message: bytes, position: int) -> tuple[int, int]:
    """Reads a base 128 varint, returning its value and the position right after it."""
    value = 0
    shift = 0
    while True:
        if position >= len(message):
            raise ValueError("varint does not fit in the message")

        byte = message[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, position

        shift += 7
        if shift > 63:
            raise ValueError("varint is longer than 64 bits")
