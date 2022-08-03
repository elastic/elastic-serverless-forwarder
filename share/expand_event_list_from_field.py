# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Iterator, Optional

import ujson

ExpandEventListFromFieldResolverCallable = Callable[[str, str], str]


class ExpandEventListFromField:
    def __init__(
        self,
        field_to_expand_event_list_from: str,
        integration_scope: str,
        field_resolver: ExpandEventListFromFieldResolverCallable,
    ):
        self.field_to_expand_event_list_from: str = field_resolver(integration_scope, field_to_expand_event_list_from)

    @staticmethod
    def _expander_event_list_from_field(
        json_object: dict[str, Any], starting_offset: int, ending_offset: int, field_to_expand_event_list_from: str
    ) -> Iterator[tuple[Any, int, int, bool]]:
        if len(field_to_expand_event_list_from) == 0 or field_to_expand_event_list_from not in json_object:
            yield {}, starting_offset, 0, False
        else:
            events_list: list[Any] = json_object[field_to_expand_event_list_from]
            # let's set to 1 if empty list to avoid division by zero in the line below,
            # for loop will be not executed anyway
            events_list_length = max(1, len(events_list))
            avg_event_length = (ending_offset - starting_offset) / events_list_length
            for event_n, event in enumerate(events_list):
                yield event, int(starting_offset + (event_n * avg_event_length)), event_n, True

    def expand(
        self, log_event: bytes, json_object: Optional[dict[str, Any]], starting_offset: int, ending_offset: int
    ) -> Iterator[tuple[bytes, int, int]]:
        if json_object is None:
            yield log_event, starting_offset, True
        else:
            for (
                expanded_event,
                expanded_starting_offset,
                expanded_event_n,
                event_was_expanded,
            ) in self._expander_event_list_from_field(
                json_object, starting_offset, ending_offset, self.field_to_expand_event_list_from
            ):
                if event_was_expanded:
                    # empty values once json dumped will have a len() greater than 0, this will prevent
                    # them to be skipped later as empty value, so we yield as zero length bytes string
                    if not expanded_event:
                        expanded_log_event = b""
                    else:
                        expanded_log_event = ujson.dumps(expanded_event).encode("utf-8")
                else:
                    expanded_log_event = log_event

                yield expanded_log_event, expanded_starting_offset, expanded_event_n
