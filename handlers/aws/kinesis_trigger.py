# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any, Iterator

from share import shared_logger
from storage import CommonStorage, StorageFactory

from .event import _default_event
from .utils import get_kinesis_stream_name_type_and_region_from_arn


def _handle_kinesis_record(kinesis_record: dict[str, Any]) -> Iterator[tuple[dict[str, Any], int]]:
    """
    Handler for kinesis data stream inputs.
    It iterates through kinesis records in the kinesis trigger and process
    the content of kinesis.data payload
    """
    storage: CommonStorage = StorageFactory.create(storage_type="payload", payload=kinesis_record["kinesis"]["data"])

    stream_type, stream_name, aws_region = get_kinesis_stream_name_type_and_region_from_arn(
        kinesis_record["eventSourceARN"]
    )

    shared_logger.info("kinesis event")

    events = storage.get_by_lines(range_start=0)

    for log_event, ending_offset, newline_length in events:
        assert isinstance(log_event, bytes)

        es_event = deepcopy(_default_event)
        es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        es_event["fields"]["message"] = log_event.decode("UTF-8")

        offset_skew = (len(log_event) + newline_length)
        log_event_tail: bytes = log_event[0 - newline_length:]
        if newline_length > 0 and (log_event_tail == b"\r\n" or log_event_tail == b"\n"):
            offset_skew -= newline_length

        es_event["fields"]["log"]["offset"] = ending_offset - offset_skew

        es_event["fields"]["log"]["file"]["path"] = kinesis_record["eventSourceARN"]

        es_event["fields"]["aws"] = {
            "kinesis": {
                "type": stream_type,
                "name": stream_name,
                "sequence_number": kinesis_record["kinesis"]["sequenceNumber"],
            }
        }

        es_event["fields"]["cloud"]["region"] = aws_region

        yield es_event, ending_offset
