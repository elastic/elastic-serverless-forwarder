# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from copy import deepcopy
from typing import Any, Optional

from share import IncludeExcludeFilter, shared_logger

from .shipper import (
    EVENT_IS_EMPTY,
    EVENT_IS_FILTERED,
    EVENT_IS_SENT,
    EventIdGeneratorCallable,
    ProtocolShipper,
    ReplayHandlerCallable,
)


class CompositeShipper:
    """
    Composite Shipper.
    This class implements composite pattern for shippers
    """

    def __init__(self, **kwargs: Any):
        self._shippers: list[ProtocolShipper] = []
        self._include_exclude_filter: Optional[IncludeExcludeFilter] = None

    def add_include_exclude_filter(self, include_exclude_filter: Optional[IncludeExcludeFilter]) -> None:
        """
        IncludeExcludeFilter setter.
        Add an includeExcludeFilter to the composite
        """
        self._include_exclude_filter = include_exclude_filter

    def add_shipper(self, shipper: ProtocolShipper) -> None:
        """
        Shipper setter.
        Add a shipper to the composite
        """
        self._shippers.append(shipper)

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        for shipper in self._shippers:
            shipper.set_event_id_generator(event_id_generator=event_id_generator)

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        for shipper in self._shippers:
            shipper.set_replay_handler(replay_handler=replay_handler)

    def send(self, event: dict[str, Any]) -> str:
        message: str = ""
        if "fields" in event and "message" in event["fields"]:
            message = event["fields"]["message"]
        elif "message" in event:
            message = event["message"]

        if len(message.strip()) == 0:
            shared_logger.debug("event is empty: message is zero length")
            return EVENT_IS_EMPTY

        if self._include_exclude_filter is not None and not self._include_exclude_filter.filter(message):
            shared_logger.debug("event is filtered according to filter rules")
            return EVENT_IS_FILTERED

        for shipper in self._shippers:
            # dict are mutated if not deep copied, every shipper can mutate the
            # events it receives without affecting the events of other shippers
            sent_event = deepcopy(event)
            shipper.send(sent_event)

        return EVENT_IS_SENT

    def flush(self) -> None:
        for shipper in self._shippers:
            shipper.flush()
