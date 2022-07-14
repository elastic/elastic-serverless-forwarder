# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Protocol, TypeVar

ReplayHandlerCallable = Callable[[str, dict[str, Any], dict[str, Any]], None]
EventIdGeneratorCallable = Callable[[dict[str, Any]], str]

EVENT_IS_EMPTY = "EVENT_IS_EMPTY"
EVENT_IS_FILTERED = "EVENT_IS_FILTERED"
EVENT_IS_SENT = "EVENT_IS_SENT"


class ProtocolShipper(Protocol):
    """
    Protocol for Shipper components
    """

    def send(self, event: dict[str, Any]) -> str:
        pass  # pragma: no cover

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        pass  # pragma: no cover

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        pass  # pragma: no cover

    def flush(self) -> None:
        pass  # pragma: no cover


ProtocolShipperType = TypeVar("ProtocolShipperType", bound=ProtocolShipper)
