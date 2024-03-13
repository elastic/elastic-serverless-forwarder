# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from copy import deepcopy
from typing import Any, Callable, Iterator, Optional, Union

from .json import json_dumper
from .logger import logger as shared_logger

# ExpandEventListFromFieldResolverCallable accepts an integration_scope and the field to expand events list from as
# arguments. It returns the resolved name of the field to expand the events list from.
ExpandEventListFromFieldResolverCallable = Callable[[str, str], str]


class ExpandEventListFromField:
    def __init__(
        self,
        field_to_expand_event_list_from: str,
        integration_scope: str,
        field_resolver: ExpandEventListFromFieldResolverCallable,
        root_fields_to_add_to_expanded_event: Optional[Union[str, list[str]]] = None,
        last_event_expanded_offset: Optional[int] = None,
    ):
        self._last_event_expanded_offset: Optional[int] = last_event_expanded_offset
        self._root_fields_to_add_to_expanded_event = root_fields_to_add_to_expanded_event
        self._field_to_expand_event_list_from: str = field_resolver(integration_scope, field_to_expand_event_list_from)

    def _expand_event_list_from_field(
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

            # Let's compute the root_fields_to_add_to_expanded_event only once per events to expand
            root_fields_to_add_to_expanded_event: dict[str, Any] = {}
            if self._root_fields_to_add_to_expanded_event == "all":
                root_fields_to_add_to_expanded_event = deepcopy(json_object)
                del root_fields_to_add_to_expanded_event[self._field_to_expand_event_list_from]
            # we want to add only a list of root fields
            elif isinstance(self._root_fields_to_add_to_expanded_event, list):
                for root_field_to_add_to_expanded_event in self._root_fields_to_add_to_expanded_event:
                    if root_field_to_add_to_expanded_event in json_object:
                        root_fields_to_add_to_expanded_event[root_field_to_add_to_expanded_event] = json_object[
                            root_field_to_add_to_expanded_event
                        ]
                    else:
                        shared_logger.debug(
                            f"`{root_field_to_add_to_expanded_event}` field specified in "
                            f"`root_fields_to_add_to_expanded_event` parameter is not present at root level"
                            f" to expanded event not present at root level"
                        )

            for event_n, event in enumerate(events_list):
                if self._root_fields_to_add_to_expanded_event:
                    # we can and want to add the root fields only in case the event is a not empty json object
                    if isinstance(event, dict) and len(event) > 0:
                        # we want to add all the root fields
                        event.update(root_fields_to_add_to_expanded_event)
                    else:
                        shared_logger.debug("root fields to be added on a non json object event")

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
            # expanded_ending_offset is set to the starting_offset because if we want to set it to the beginning of the
            # json object in case of a message from the continuation queue. if we update it, if the payload is continued
            # we will fetch the content of the payload from the middle of the json object, failing to parse it
            expanded_ending_offset: int = starting_offset

            for (
                expanded_event,
                expanded_starting_offset,
                expanded_event_n,
                is_last_expanded_event,
                event_was_expanded,
            ) in self._expand_event_list_from_field(json_object, starting_offset, ending_offset):
                if event_was_expanded:
                    # empty values once json dumped might have a len() greater than 0, this will prevent
                    # them to be skipped later as empty value, so we yield as zero length bytes string
                    if not expanded_event:
                        expanded_log_event = b""
                    else:
                        expanded_log_event = json_dumper(expanded_event).encode("utf-8")

                    if is_last_expanded_event:
                        expanded_event_n = None
                        # only when we reach the last expanded event we can move the ending offset
                        expanded_ending_offset = ending_offset
                else:
                    expanded_event_n = None
                    expanded_log_event = log_event
                    expanded_ending_offset = ending_offset

                yield expanded_log_event, expanded_starting_offset, expanded_ending_offset, expanded_event_n
