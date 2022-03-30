# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Iterator
from unittest import TestCase

import pytest

from storage import CommonStorage


class DummyStorage(CommonStorage):
    def get_as_string(self) -> str:
        return ""

    def get_by_lines(self, range_start: int) -> Iterator[tuple[bytes, int, int, int]]:
        yield b"", 0, 0, 0

    def __init__(self, **kwargs: Any):
        pass


@pytest.mark.unit
class TestCommonStorage(TestCase):
    def test_init(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonStorage.__init__(DummyStorage())

    def test_get_as_string(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonStorage.get_as_string(DummyStorage())

    def test_get_by_lines(self) -> None:
        with self.assertRaises(NotImplementedError):
            CommonStorage.get_by_lines(DummyStorage(), 0)
