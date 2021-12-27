# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any
from unittest import TestCase

import pytest

from shippers import CommonShipper


class DummyShipper(CommonShipper):
    def send(self, event: dict[str, Any]) -> Any:
        return

    def flush(self) -> None:
        pass

    def discover_dataset(self, event: dict[str, Any]) -> None:
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

    def test_flush(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonShipper.flush(DummyShipper())

    def test_discover_dataset(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonShipper.discover_dataset(DummyShipper(), {})
