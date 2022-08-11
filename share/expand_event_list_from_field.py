# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Iterator, Optional

from .json import json_dumper

ExpandEventListFromFieldResolverCallable = Callable[[str, str], str]


class ExpandEventListFromField:
    def __init__(
        self,
        field_to_expand_event_list_from: str,
        integration_scope: str,
        field_resolver: ExpandEventListFromFieldResolverCallable,
        last_event_expanded_offset: Optional[int] = None,
    ):
        self._last_event_expanded_offset: Optional[int] = last_event_expanded_offset
        self._field_to_expand_event_list_from: str = field_resolver(integration_scope, field_to_expand_event_list_from)

    def _expander_event_list_from_field(
        self, json_object: dict[str, Any], starting_offset: int, ending_offset: int
    ) -> Iterator[tuple[Any, int, Optional[int], bool, bool]]:
        if len(self._field_to_expand_event_list_from) == 0 or self._field_to_expand_event_list_from not in json_object:
            yield None, starting_offset, 0, True, False
        else:
            events_list: list[Any] = json_object[self._field_to_expand_event_list_from]
            # let's set to 1 if empty list to avoid division by zero in the line below,
            # for loop will be not executed anyway
            offset_skew = 0
            events_list_length = max(1, len(events_list))
            avg_event_length = (ending_offset - starting_offset) / events_list_length
            if self._last_event_expanded_offset is not None and len(events_list) > self._last_event_expanded_offset + 1:
                offset_skew = self._last_event_expanded_offset + 1
                events_list = events_list[offset_skew:]

            for event_n, event in enumerate(events_list):
                event_n += offset_skew
                yield event, int(
                    starting_offset + (event_n * avg_event_length)
                ), event_n, event_n == events_list_length - 1, True

    def expand(
        self, log_event: bytes, json_object: Optional[dict[str, Any]], starting_offset: int, ending_offset: int
    ) -> Iterator[tuple[bytes, int, int, Optional[int]]]:
        if json_object is None:
            yield log_event, starting_offset, ending_offset, None
        else:
            expanded_ending_offset: int = starting_offset

            for (
                expanded_event,
                expanded_starting_offset,
                expanded_event_n,
                is_last_expanded_event,
                event_was_expanded,
            ) in self._expander_event_list_from_field(json_object, starting_offset, ending_offset):
                if event_was_expanded:
                    # empty values once json dumped will have a len() greater than 0, this will prevent
                    # them to be skipped later as empty value, so we yield as zero length bytes string
                    if not expanded_event:
                        expanded_log_event = b""
                    else:
                        expanded_log_event = json_dumper(expanded_event).encode("utf-8")

                    if is_last_expanded_event:
                        expanded_event_n = None
                        expanded_ending_offset = ending_offset
                else:
                    expanded_event_n = None
                    expanded_log_event = log_event
                    expanded_ending_offset = ending_offset

                yield expanded_log_event, expanded_starting_offset, expanded_ending_offset, expanded_event_n
