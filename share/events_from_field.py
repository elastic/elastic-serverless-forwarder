# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Iterator, Optional

import ujson

ExtractEventsFromFieldCallable = Callable[
    [dict[str, Any], int, int, str], Iterator[tuple[dict[str, Any], int, bool, bool]]
]


def extract_events_from_field(
    log_event: bytes,
    json_object: Optional[dict[str, Any]],
    starting_offset: int,
    ending_offset: int,
    integration_scope: str,
    extractor: ExtractEventsFromFieldCallable,
) -> Iterator[tuple[bytes, int, bool]]:
    if json_object is None:
        yield log_event, starting_offset, True
    else:
        for extracted_event, extracted_starting_offset, is_last_event_extracted, event_was_extracted in extractor(
            json_object, starting_offset, ending_offset, integration_scope
        ):
            if event_was_extracted:
                extracted_log_event = ujson.dumps(extracted_event).encode("utf-8")
            else:
                extracted_log_event = log_event

            yield extracted_log_event, extracted_starting_offset, is_last_event_extracted
