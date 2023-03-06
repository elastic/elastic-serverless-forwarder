# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any
from unittest import TestCase

import pytest

from share import IncludeExcludeFilter, IncludeExcludeRule
from shippers import (
    EVENT_IS_EMPTY,
    EVENT_IS_FILTERED,
    EVENT_IS_SENT,
    CompositeShipper,
    EventIdGeneratorCallable,
    ReplayHandlerCallable,
)


class DummyShipper:
    def send(self, event: dict[str, Any]) -> str:
        self._sent.append(event)
        return "dummy"

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        self._event_id_generator = event_id_generator

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        self._replay_handler = replay_handler

    def flush(self) -> None:
        self._flushed = True

    def __init__(self, **kwargs: Any):
        self._sent: list[dict[str, Any]] = []
        self._flushed = False


@pytest.mark.unit
class TestCompositeShipper(TestCase):
    def test_add_shipper(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)
        assert composite_shipper._shippers == [dummy_shipper]

    def test_add_include_exclude_filter(self) -> None:
        composite_shipper = CompositeShipper()
        include_exclude_filter = IncludeExcludeFilter()
        composite_shipper.add_include_exclude_filter(include_exclude_filter)
        assert composite_shipper._include_exclude_filter == include_exclude_filter

    def test_send(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)
        assert EVENT_IS_EMPTY == composite_shipper.send({"miss": "message field"})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"fields": {"message": ""}})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"message": ""})
        assert dummy_shipper._sent == []

        assert EVENT_IS_SENT == composite_shipper.send({"message": "will pass"})
        assert dummy_shipper._sent == [{"message": "will pass"}]

        dummy_shipper._sent = []

        assert EVENT_IS_SENT == composite_shipper.send({"fields": {"message": "will pass"}})
        assert dummy_shipper._sent == [{"fields": {"message": "will pass"}}]

        dummy_shipper._sent = []

        include_exclude_filter = IncludeExcludeFilter(include_patterns=[IncludeExcludeRule(pattern="match")])
        composite_shipper.add_include_exclude_filter(include_exclude_filter)

        assert EVENT_IS_EMPTY == composite_shipper.send({"miss": "message field"})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"fields": {"message": ""}})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"message": ""})
        assert dummy_shipper._sent == []

        assert EVENT_IS_SENT == composite_shipper.send({"fields": {"message": "match"}})
        assert dummy_shipper._sent == [{"fields": {"message": "match"}}]

        dummy_shipper._sent = []

        include_exclude_filter = IncludeExcludeFilter(include_patterns=[IncludeExcludeRule(pattern="match")])
        composite_shipper.add_include_exclude_filter(include_exclude_filter)
        assert EVENT_IS_SENT == composite_shipper.send({"message": "match"})
        assert dummy_shipper._sent == [{"message": "match"}]

        dummy_shipper._sent = []

        assert EVENT_IS_EMPTY == composite_shipper.send({"miss": "message field"})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"fields": {"message": ""}})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"message": ""})
        assert dummy_shipper._sent == []

        include_exclude_filter = IncludeExcludeFilter(include_patterns=[IncludeExcludeRule(pattern="not match")])
        composite_shipper.add_include_exclude_filter(include_exclude_filter)

        assert EVENT_IS_EMPTY == composite_shipper.send({"miss": "message field"})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"fields": {"message": ""}})
        assert dummy_shipper._sent == []

        assert EVENT_IS_EMPTY == composite_shipper.send({"message": ""})
        assert dummy_shipper._sent == []

        assert EVENT_IS_FILTERED == composite_shipper.send({"fields": {"message": "a message"}})
        assert dummy_shipper._sent == []

        dummy_shipper._sent = []

        assert EVENT_IS_FILTERED == composite_shipper.send({"message": "a message"})
        assert dummy_shipper._sent == []

    def test_set_event_id_generator(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)

        def event_id_generator(event: dict[str, Any]) -> str:
            return ""

        composite_shipper.set_event_id_generator(event_id_generator=event_id_generator)
        assert dummy_shipper._event_id_generator == event_id_generator

    def test_set_replay_handler(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)

        def replay_handler(output_type: str, output_args: dict[str, Any], payload: dict[str, Any]) -> None:
            return

        composite_shipper.set_replay_handler(replay_handler=replay_handler)
        assert dummy_shipper._replay_handler == replay_handler

    def test_flush(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)
        composite_shipper.flush()
        assert dummy_shipper._flushed is True
