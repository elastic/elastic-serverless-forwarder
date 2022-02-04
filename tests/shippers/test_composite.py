# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any
from unittest import TestCase

import pytest

from shippers import CommonShipper, CompositeShipper, EventIdGeneratorCallable, ReplayHandlerCallable


class DummyShipper(CommonShipper):
    def send(self, event: dict[str, Any]) -> Any:
        self._sent.append(event)
        return

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

    def test_send(self) -> None:
        dummy_shipper = DummyShipper()
        composite_shipper = CompositeShipper()
        composite_shipper.add_shipper(dummy_shipper)
        composite_shipper.send({})
        assert dummy_shipper._sent == [{}]

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
