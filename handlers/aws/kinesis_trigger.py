# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any, Iterator

from share import extract_events_from_field, shared_logger
from storage import CommonStorage, StorageFactory

from .event import _default_event
from .utils import extractor_events_from_field, get_kinesis_stream_name_type_and_region_from_arn


def _handle_kinesis_record(
    kinesis_record: dict[str, Any], integration_scope: str, account_id: str
) -> Iterator[tuple[dict[str, Any], int]]:
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

    for log_event, json_object, ending_offset, starting_offset, newline_length in events:
        assert isinstance(log_event, bytes)

        for extracted_log_event, extracted_starting_offset, _ in extract_events_from_field(
            log_event, json_object, starting_offset, ending_offset, integration_scope, extractor_events_from_field
        ):
            es_event = deepcopy(_default_event)
            es_event["@timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            es_event["fields"]["message"] = extracted_log_event.decode("UTF-8")

            es_event["fields"]["log"]["offset"] = extracted_starting_offset

            es_event["fields"]["log"]["file"]["path"] = kinesis_record["eventSourceARN"]

            es_event["fields"]["aws"] = {
                "kinesis": {
                    "type": stream_type,
                    "name": stream_name,
                    "sequence_number": kinesis_record["kinesis"]["sequenceNumber"],
                }
            }

            es_event["fields"]["cloud"]["region"] = aws_region
            es_event["fields"]["cloud"]["account"] = {"id": account_id}

            yield es_event, ending_offset
