# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from io import BytesIO
from typing import Optional, Union
from unittest import TestCase

import pytest

from share import ExpandEventListFromField, MultilineFactory, ProtocolMultiline
from storage import (
    CommonStorage,
    GetByLinesIterator,
    StorageDecoratorIterator,
    StorageReader,
    by_lines,
    inflate,
    json_collector,
    multi_line,
)


class DummyStorage(CommonStorage):
    """
    Dummy Storage.
    """

    def __init__(
        self,
        json_content_type: Optional[str] = None,
        multiline_processor: Optional[ProtocolMultiline] = None,
        event_list_from_field_expander: Optional[ExpandEventListFromField] = None,
    ):
        self.json_content_type = json_content_type
        self.multiline_processor = multiline_processor
        self.event_list_from_field_expander = event_list_from_field_expander

    @staticmethod
    def get_by_lines(range_start: int) -> GetByLinesIterator:
        yield b"", 0, 0, None

    @staticmethod
    def get_as_string() -> str:
        return ""

    @multi_line
    @json_collector
    @by_lines
    @inflate
    def generate(self, range_start: int, body: BytesIO, is_gzipped: bool) -> StorageDecoratorIterator:
        if is_gzipped:
            reader: StorageReader = StorageReader(raw=body)
            yield reader, 0, 0, b"", None
        else:
            yield body.read(), 0, 0, b"", None


@pytest.mark.unit
class TestDecorator(TestCase):
    def test_plain(self) -> None:
        storage = DummyStorage()
        fixtures = BytesIO(b"line1\nline2\nline3\n")
        expected = [
            (b"line1", 0, 6, b"\n", None),
            (b"line2", 6, 12, b"\n", None),
            (b"line3", 12, 18, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_ndjson(self) -> None:
        storage = DummyStorage(json_content_type="ndjson")
        fixtures = BytesIO(b'{"line": 1}\n{"line": 2}\n{"line": 3}\n')
        expected = [
            (b'{"line": 1}', 0, 12, b"\n", None),
            (b'{"line": 2}', 12, 24, b"\n", None),
            (b'{"line": 3}', 24, 36, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_single(self) -> None:
        storage = DummyStorage(json_content_type="single")
        fixtures = BytesIO(b'{"line1": 1,\n"line2": 2,\n"line3": 3}\n')
        expected = [
            (b'{"line1": 1,\n"line2": 2,\n"line3": 3}', 0, 37, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_gzip(self) -> None:
        storage = DummyStorage()
        fixtures = BytesIO(gzip.compress(b"line1\nline2\nline3\n"))
        expected = [
            (b"line1", 0, 6, b"\n", None),
            (b"line2", 6, 12, b"\n", None),
            (b"line3", 12, 18, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, True
                )
            ]
        )

        assert expected == decorated

    def test_expand_event_list_from_field(self) -> None:
        def resolver(_: str, field_to_expand_event_list_from: str) -> str:
            return field_to_expand_event_list_from

        event_list_from_field_expander = ExpandEventListFromField("Records", "", resolver, None, None)

        storage = DummyStorage(
            event_list_from_field_expander=event_list_from_field_expander, json_content_type="single"
        )
        fixtures = BytesIO(b'{"Records": [{"line": 1},\n{"line": 2},\n{"line": 3}\n]}\n')
        expected = [
            (b'{"line":1}', 0, 0, b"\n", 0),  # ending_offset is not set with event list from field expander
            (b'{"line":2}', 18, 0, b"\n", 1),  # ending_offset is not set with event list from field expander
            (b'{"line":3}', 36, 54, b"\n", None),  # ending_offset is set only for the last expanded event
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_expand_event_list_from_field_with_offset(self) -> None:
        def resolver(_: str, field_to_expand_event_list_from: str) -> str:
            return field_to_expand_event_list_from

        event_list_from_field_expander = ExpandEventListFromField("Records", "", resolver, None, 0)

        storage = DummyStorage(
            event_list_from_field_expander=event_list_from_field_expander, json_content_type="single"
        )
        fixtures = BytesIO(b'{"Records": [{"line": 1},\n{"line": 2},\n{"line": 3}\n]}\n')
        expected = [
            (b'{"line":2}', 18, 0, b"\n", 1),
            (b'{"line":3}', 36, 54, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_expand_event_list_from_field_json_content_type_single_no_circuit_breaker(self) -> None:
        def resolver(_: str, field_to_expand_event_list_from: str) -> str:
            return field_to_expand_event_list_from

        event_list_from_field_expander = ExpandEventListFromField("Records", "", resolver, None, None)

        storage = DummyStorage(
            event_list_from_field_expander=event_list_from_field_expander, json_content_type="single"
        )
        data: bytes = (
            b'{"Records": ['
            + b",\n".join([b'{"a line":"' + str(i).encode("utf-8") + b'"}' for i in range(0, 2000)])
            + b"]}\n"
        )
        fixtures = BytesIO(data)
        expected: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (b'{"a line":"' + str(i).encode("utf-8") + b'"}', int(i * (len(data) / 2000)), 0, b"\n", i)
                for i in range(0, 2000)
            ]
        )
        expected.pop()
        expected.append((b'{"a line":"1999"}', int(len(data) - (len(data) / 2000)), len(data), b"\n", None))

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated

    def test_multiline_processor(self) -> None:
        multiline_processor = MultilineFactory.create(multiline_type="count", count_lines=3)

        storage = DummyStorage(multiline_processor=multiline_processor, json_content_type="single")
        fixtures = BytesIO(b"line1\nline2\nline3\n")
        expected = [
            (b"line1\nline2\nline3", 0, 18, b"\n", None),
        ]

        decorated: list[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]] = list(
            [
                (data, starting_offset, ending_offset, newline, event_expanded_offset)
                for data, starting_offset, ending_offset, newline, event_expanded_offset in storage.generate(
                    0, fixtures, False
                )
            ]
        )

        assert expected == decorated
