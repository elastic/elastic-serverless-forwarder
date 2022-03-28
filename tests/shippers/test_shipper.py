# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any
from unittest import TestCase

import pytest

from shippers import CommonShipper, EventIdGeneratorCallable, ReplayHandlerCallable


class DummyShipper(CommonShipper):
    def send(self, event: dict[str, Any]) -> bool:
        pass

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        pass

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        pass

    def flush(self) -> None:
        pass

    def __init__(self, **kwargs: Any):
        pass


@pytest.mark.unit
class TestCommonShipper(TestCase):
    def test_init(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonShipper.__init__(DummyShipper())

    def test_send(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonShipper.send(DummyShipper(), {})

    def test_set_event_id_generator(self) -> None:
        with self.assertRaises(NotImplementedError):

            def event_id_generator(event: dict[str, Any]) -> str:
                return ""

            CommonShipper.set_event_id_generator(DummyShipper(), event_id_generator=event_id_generator)

    def test_set_replay_handler(self) -> None:
        with self.assertRaises(NotImplementedError):

            def replay_handler(output_type: str, output_args: dict[str, Any], payload: dict[str, Any]) -> None:
                return

            CommonShipper.set_replay_handler(DummyShipper(), replay_handler=replay_handler)

    def test_flush(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonShipper.flush(DummyShipper())
