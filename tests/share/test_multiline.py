# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import datetime
from typing import Optional
from unittest import TestCase

import mock
import pytest

from share import CollectBuffer, CountMultiline, FeedIterator, PatternMultiline, WhileMultiline

collect_buffer_grow = [
    pytest.param(
        False, [b"line1", b"line2"], b"\n", b"line1\nline2", 12, id="concatenating two events with newlines: \n"
    ),
    pytest.param(
        False, [b"line1", b"line2"], b"\r\n", b"line1\r\nline2", 14, id="concatenating two events with newlines: \r\n"
    ),
    pytest.param(
        True,
        [b'{"key1": "value",', b'"key2": "value"}'],
        b"",
        b'{"key1": "value","key2": "value"}',
        33,
        id="concatenating two events without newlines",
    ),
    pytest.param(
        True,
        [b'{"key1": "value",', b'"key2": "value"}'],
        b"",
        b'{"key1": "value","key2": "value"}',
        33,
        id="concatenating two events without newlines",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("skip_newline,lines,newline,expected_content,expected_content_length", collect_buffer_grow)
def test_collect_buffer_grow(
    skip_newline: bool, lines: list[bytes], newline: bytes, expected_content: bytes, expected_content_length: int
) -> None:
    collect_buffer = CollectBuffer(max_bytes=1024, max_lines=5, skip_newline=skip_newline)

    for i, line in enumerate(lines):
        collect_buffer.grow(data=line, newline=newline)

    content, content_length, _ = collect_buffer.collect_and_reset()

    assert content == expected_content
    assert content_length == expected_content_length


collect_buffer_collect = [
    pytest.param(1024, [b"one line"], b"", b"one line", 8, id="one liner with no flags"),
    pytest.param(1024, [b"one line"], b"\n", b"one line", 9, id="one liner with no flags: \n"),
    pytest.param(1024, [b"one line"], b"\r\n", b"one line", 10, id="one liner with no flags: \r\n"),
    pytest.param(
        20,
        [b"tooooooooooooooooooo looooooong line"],
        b"",
        b"tooooooooooooooooooo",
        36,
        id="truncated one liner message",
    ),
    pytest.param(
        20,
        [b"tooooooooooooooooooo looooooong line"],
        b"\n",
        b"tooooooooooooooooooo",
        37,
        id="truncated one liner message: \n",
    ),
    pytest.param(
        20,
        [b"tooooooooooooooooooo looooooong line"],
        b"\r\n",
        b"tooooooooooooooooooo",
        38,
        id="truncated one liner message: \r\n",
    ),
    pytest.param(1024, [b"line1", b"line2"], b"\n", b"line1\nline2", 12, id="untruncated multiline message: \n"),
    pytest.param(1024, [b"line1", b"line2"], b"\r\n", b"line1\r\nline2", 14, id="untruncated multiline message: \r\n"),
    pytest.param(8, [b"line1", b"line2"], b"\n", b"line1\nlin", 12, id="truncated multiline message: \n"),
    pytest.param(8, [b"line1", b"line2"], b"\r\n", b"line1\r\nlin", 14, id="truncated multiline message: \r\n"),
    pytest.param(0, [b"line1", b"line2"], b"\n", b"", 12, id="zero length truncated multiline message: \n"),
    pytest.param(0, [b"line1", b"line2"], b"\r\n", b"", 14, id="zero length truncated multiline message: \r\n"),
    pytest.param(3, [b"line1", b"line2"], b"\n", b"lin", 12, id="zero length truncated multiline message: \n"),
    pytest.param(3, [b"line1", b"line2"], b"\r\n", b"lin", 14, id="zero length truncated multiline message: \r\n"),
]


@pytest.mark.unit
@pytest.mark.parametrize("max_bytes,lines,newline,expected_content,expected_content_length", collect_buffer_collect)
def test_collect_buffer_collect(
    max_bytes: int, lines: list[bytes], newline: bytes, expected_content: bytes, expected_content_length: int
) -> None:
    collect_buffer = CollectBuffer(max_bytes=max_bytes, max_lines=5, skip_newline=False)
    for i, line in enumerate(lines):
        collect_buffer.grow(data=line, newline=newline)

    content, content_length, _ = collect_buffer.collect_and_reset()

    assert content == expected_content
    assert content_length == expected_content_length


pattern_multiline_collect = [
    pytest.param(
        b"\n",
        b"line1\n  line1.1\n  line1.2\nline2\n  line2.1\n  line2.2\n",
        "^[ \t] +",
        "after",
        "",
        False,
        None,
        [(b"line1\n  line1.1\n  line1.2", 26), (b"line2\n  line2.1\n  line2.2", 26)],
        id="after: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n  line1.1\r\n  line1.2\r\nline2\r\n  line2.1\r\n  line2.2\r\n",
        "^[ \t] +",
        "after",
        "",
        False,
        None,
        [(b"line1\r\n  line1.1\r\n  line1.2", 29), (b"line2\r\n  line2.1\r\n  line2.2", 29)],
        id="after: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1 \\\nline1.1 \\\nline1.2\nline2 \\\nline2.1 \\\nline2.2\n",
        "\\\\$",
        "before",
        "",
        False,
        None,
        [(b"line1 \\\nline1.1 \\\nline1.2", 26), (b"line2 \\\nline2.1 \\\nline2.2", 26)],
        id="before: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1 \\\r\nline1.1 \\\r\nline1.2\r\nline2 \\\r\nline2.1 \\\r\nline2.2\r\n",
        "\\\\$",
        "before",
        "",
        False,
        None,
        [(b"line1 \\\r\nline1.1 \\\r\nline1.2", 29), (b"line2 \\\r\nline2.1 \\\r\nline2.2", 29)],
        id="before: \r\n",
    ),
    pytest.param(
        b"\n",
        b"-line1\n  - line1.1\n  - line1.2\n-line2\n  - line2.1\n  - line2.2\n",
        "^-",
        "after",
        "",
        True,
        None,
        [(b"-line1\n  - line1.1\n  - line1.2", 31), (b"-line2\n  - line2.1\n  - line2.2", 31)],
        id="after negate: \n",
    ),
    pytest.param(
        b"\r\n",
        b"-line1\r\n  - line1.1\r\n  - line1.2\r\n-line2\r\n  - line2.1\r\n  - line2.2\r\n",
        "^-",
        "after",
        "",
        True,
        None,
        [(b"-line1\r\n  - line1.1\r\n  - line1.2", 34), (b"-line2\r\n  - line2.1\r\n  - line2.2", 34)],
        id="after negate: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\nline1.1\nline1.2;\nline2\nline2.1\nline2.2;\n",
        ";$",
        "before",
        "",
        True,
        None,
        [(b"line1\nline1.1\nline1.2;", 23), (b"line2\nline2.1\nline2.2;", 23)],
        id="before negate: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\nline1.1\r\nline1.2;\r\nline2\r\nline2.1\r\nline2.2;\r\n",
        ";$",
        "before",
        "",
        True,
        None,
        [(b"line1\r\nline1.1\r\nline1.2;", 26), (b"line2\r\nline2.1\r\nline2.2;", 26)],
        id="before negate: \r\n",
    ),
    pytest.param(
        b"\n",
        b"EventStart\nEventId: 1\nEventEnd\nOtherThingInBetween\nEventStart\nEventId: 2\nEventEnd\n",
        "EventStart",
        "after",
        "EventEnd",
        True,
        None,
        [
            (b"EventStart\nEventId: 1\nEventEnd", 31),
            (b"OtherThingInBetween", 20),
            (b"EventStart\nEventId: 2\nEventEnd", 31),
        ],
        id="after negate flush pattern: \n",
    ),
    pytest.param(
        b"\r\n",
        b"EventStart\r\nEventId: 1\r\nEventEnd\r\nOtherThingInBetween\r\nEventStart\r\nEventId: 2\r\nEventEnd\r\n",
        "EventStart",
        "after",
        "EventEnd",
        True,
        None,
        [
            (b"EventStart\r\nEventId: 1\r\nEventEnd", 34),
            (b"OtherThingInBetween", 21),
            (b"EventStart\r\nEventId: 2\r\nEventEnd", 34),
        ],
        id="after negate flush pattern: \r\n",
    ),
    pytest.param(
        b"\n",
        b"StartLineThatDoesntMatchTheEvent\nOtherThingInBetween\nEventStart\n"
        b"EventId: 2\nEventEnd\nEventStart\nEventId: 3\nEventEnd\n",
        "EventStart",
        "after",
        "EventEnd",
        True,
        None,
        [
            (b"StartLineThatDoesntMatchTheEvent\nOtherThingInBetween", 53),
            (b"EventStart\nEventId: 2\nEventEnd", 31),
            (b"EventStart\nEventId: 3\nEventEnd", 31),
        ],
        id="after negate flush pattern, first line doesn't match: \n",
    ),
    pytest.param(
        b"\r\n",
        b"StartLineThatDoesntMatchTheEvent\r\nOtherThingInBetween\r\nEventStart\r\n"
        b"EventId: 2\r\nEventEnd\r\nEventStart\r\nEventId: 3\r\nEventEnd\r\n",
        "EventStart",
        "after",
        "EventEnd",
        True,
        None,
        [
            (b"StartLineThatDoesntMatchTheEvent\r\nOtherThingInBetween", 55),
            (b"EventStart\r\nEventId: 2\r\nEventEnd", 34),
            (b"EventStart\r\nEventId: 3\r\nEventEnd", 34),
        ],
        id="after negate flush pattern, first line doesn't match: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n\n\nline1.2;\nline2\nline2.1\nline2.2;\n",
        ";$",
        "before",
        "",
        True,
        None,
        [(b"line1\n\n\nline1.2;", 17), (b"line2\nline2.1\nline2.2;", 23)],
        id="before negate, with empty line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n\r\n\r\nline1.2;\r\nline2\r\nline2.1\r\nline2.2;\r\n",
        ";$",
        "before",
        "",
        True,
        None,
        [(b"line1\r\n\r\n\r\nline1.2;", 21), (b"line2\r\nline2.1\r\nline2.2;", 26)],
        id="before negate, with empty line: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n line1.1\n line1.2\nline2\n line2.1\n line2.2\n",
        "^[ ]",
        "after",
        "",
        False,
        2,
        [(b"line1\n line1.1", 24), (b"line2\n line2.1", 24)],
        id="after truncated: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\n line1.2\r\nline2\r\n line2.1\r\n line2.2\r\n",
        "^[ ]",
        "after",
        "",
        False,
        2,
        [(b"line1\r\n line1.1", 27), (b"line2\r\n line2.1", 27)],
        id="after truncated: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n line1.1\nline2\n line2.1\n",
        "^[ ]",
        "after",
        "",
        False,
        2,
        [(b"line1\n line1.1", 15), (b"line2\n line2.1", 15)],
        id="after not truncated: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\nline2\r\n line2.1\r\n",
        "^[ ]",
        "after",
        "",
        False,
        2,
        [(b"line1\r\n line1.1", 17), (b"line2\r\n line2.1", 17)],
        id="after not truncated: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n\nline1.1\nline2\n\nline2.1\n",
        "^.{0,0}$",
        "after",
        "",
        False,
        None,
        [(b"line1\n\nline1.1", 15), (b"line2\n\nline2.1", 15)],
        id="after match empty line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n\r\nline1.1\r\nline2\r\n\r\nline2.1\r\n",
        "^.{0,0}$",
        "after",
        "",
        False,
        None,
        [(b"line1\r\n\r\nline1.1", 18), (b"line2\r\n\r\nline2.1", 18)],
        id="after match empty line: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1 \n\nline1.1 \n\nline1.2\nline2 \n\nline2.1 \n\nline2.2\n",
        "^.{0,0}$",
        "before",
        "",
        False,
        None,
        [
            (b"line1 ", 7),
            (b"\nline1.1 ", 10),
            (b"\nline1.2", 9),
            (b"line2 ", 7),
            (b"\nline2.1 ", 10),
            (b"\nline2.2", 9),
        ],
        id="before match empty line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1 \r\n\r\nline1.1 \r\n\r\nline1.2\r\nline2 \r\n\r\nline2.1 \r\n\r\nline2.2\r\n",
        "^.{0,0}$",
        "before",
        "",
        False,
        None,
        [
            (b"line1 ", 8),
            (b"\r\nline1.1 ", 12),
            (b"\r\nline1.2", 11),
            (b"line2 ", 8),
            (b"\r\nline2.1 ", 12),
            (b"\r\nline2.2", 11),
        ],
        id="before match empty line: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    "newline,feed,pattern,match,flush_pattern,negate,max_lines,expected_events",
    pattern_multiline_collect,
)
def test_pattern_multiline(
    newline: bytes,
    feed: bytes,
    pattern: str,
    match: str,
    flush_pattern: str,
    negate: bool,
    max_lines: Optional[int],
    expected_events: list[tuple[bytes, int]],
) -> None:
    if max_lines is None:
        max_lines = 5

    pattern_multiline: PatternMultiline = PatternMultiline(
        pattern=pattern, match=match, flush_pattern=flush_pattern, negate=negate, max_lines=max_lines
    )

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    pattern_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in pattern_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


pattern_multiline_collect_circuitbreaker = [
    pytest.param(
        b"\n",
        b"line1\n  line1.1\n  line1.2\nline2\n  line2.1\n  line2.2\n",
        [(b"line1", 6), (b"  line1.1", 10), (b"  line1.2", 10), (b"line2", 6), (b"  line2.1", 10), (b"  line2.2", 10)],
        id="circuit breaker: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n  line1.1\r\n  line1.2\r\nline2\r\n  line2.1\r\n  line2.2\r\n",
        [(b"line1", 7), (b"  line1.1", 11), (b"  line1.2", 11), (b"line2", 7), (b"  line2.1", 11), (b"  line2.2", 11)],
        id="circuit breaker: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("newline,feed,expected_events", pattern_multiline_collect_circuitbreaker)
@mock.patch("share.multiline.timedelta_circuit_breaker", new=datetime.timedelta(seconds=-1))
def test_pattern_multiline_circuitbreaker(
    newline: bytes,
    feed: bytes,
    expected_events: list[tuple[bytes, int]],
) -> None:
    pattern_multiline: PatternMultiline = PatternMultiline(pattern="^[ \t] +", match="after")

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    pattern_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in pattern_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


count_multiline_collect = [
    pytest.param(
        b"\n",
        b"line1\n line1.1\nline2\n line2.1\n",
        2,
        2,
        [(b"line1\n line1.1", 15), (b"line2\n line2.1", 15)],
        id="count_lines 2, max_lines 2: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\nline2\r\n line2.1\r\n",
        2,
        2,
        [(b"line1\r\n line1.1", 17), (b"line2\r\n line2.1", 17)],
        id="count_lines 2, max_lines 2: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n line1.1\nline2\n line2.1\nline3\n line3.1\nline4\n line4.1\n",
        4,
        4,
        [(b"line1\n line1.1\nline2\n line2.1", 30), (b"line3\n line3.1\nline4\n line4.1", 30)],
        id="count_lines 4, max_lines 4: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\nline2\r\n line2.1\r\nline3\r\n line3.1\r\nline4\r\n line4.1\r\n",
        4,
        4,
        [(b"line1\r\n line1.1\r\nline2\r\n line2.1", 34), (b"line3\r\n line3.1\r\nline4\r\n line4.1", 34)],
        id="count_lines 4, max_lines 4: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\nline1.1\nline2\nline2.1\nline3\nline3.1\nline4\nline4.1\n",
        1,
        1,
        [
            (b"line1", 6),
            (b"line1.1", 8),
            (b"line2", 6),
            (b"line2.1", 8),
            (b"line3", 6),
            (b"line3.1", 8),
            (b"line4", 6),
            (b"line4.1", 8),
        ],
        id="count_lines 1, max_lines 1: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\nline1.1\r\nline2\r\nline2.1\r\nline3\r\nline3.1\r\nline4\r\nline4.1\r\n",
        1,
        1,
        [
            (b"line1", 7),
            (b"line1.1", 9),
            (b"line2", 7),
            (b"line2.1", 9),
            (b"line3", 7),
            (b"line3.1", 9),
            (b"line4", 7),
            (b"line4.1", 9),
        ],
        id="count_lines 1, max_lines 1: \r\n",
    ),
    pytest.param(
        b"\n",
        b"line1\n line1.1\n line1.2\nline2\n line2.1\n line2.2\n"
        b"line3\n line3.1\n line3.2\nline4\n line4.1\n line4.3\n"
        b"line5\r\n line5.1\r\n",
        3,
        2,
        [
            (b"line1\n line1.1", 24),
            (b"line2\n line2.1", 24),
            (b"line3\n line3.1", 24),
            (b"line4\n line4.1", 24),
            (b"line5\n line5.1", 15),
        ],
        id="count_lines 3, max_lines 2: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\n line1.2\r\nline2\r\n line2.1\r\n line2.2\r\n"
        b"line3\r\n line3.1\r\n line3.2\r\nline4\r\n line4.1\r\n line4.3\r\n"
        b"line5\r\n line5.1\r\n",
        3,
        2,
        [
            (b"line1\r\n line1.1", 27),
            (b"line2\r\n line2.1", 27),
            (b"line3\r\n line3.1", 27),
            (b"line4\r\n line4.1", 27),
            (b"line5\r\n line5.1", 17),
        ],
        id="count_lines 3, max_lines 2: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("newline,feed,count_lines,max_lines,expected_events", count_multiline_collect)
def test_count_multiline(
    newline: bytes, feed: bytes, count_lines: int, max_lines: Optional[int], expected_events: list[tuple[bytes, int]]
) -> None:
    if max_lines is None:
        max_lines = 5

    count_multiline: CountMultiline = CountMultiline(count_lines=count_lines, max_lines=max_lines)

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    count_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in count_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


count_multiline_collect_circuitbreaker = [
    pytest.param(
        b"\n",
        b"line1\n line1.1\nline2\n line2.1\n",
        [(b"line1", 6), (b" line1.1", 9), (b"line2", 6), (b" line2.1", 9)],
        id="circuit breaker: \n",
    ),
    pytest.param(
        b"\r\n",
        b"line1\r\n line1.1\r\nline2\r\n line2.1\r\n",
        [(b"line1", 7), (b" line1.1", 10), (b"line2", 7), (b" line2.1", 10)],
        id="circuit breaker: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("newline,feed,expected_events", count_multiline_collect_circuitbreaker)
@mock.patch("share.multiline.timedelta_circuit_breaker", new=datetime.timedelta(seconds=0))
def test_count_multiline_circuitbreaker(newline: bytes, feed: bytes, expected_events: list[tuple[bytes, int]]) -> None:
    count_multiline: CountMultiline = CountMultiline(count_lines=2)

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    count_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in count_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


while_multiline_collect = [
    pytest.param(
        b"\n",
        b"{line1\n{line1.1\nnot matched line\n{line2\n{line2.1\n",
        "^{",
        False,
        None,
        [(b"{line1\n{line1.1", 16), (b"not matched line", 17), (b"{line2\n{line2.1", 16)],
        id="not matched line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{line1\r\n{line1.1\r\nnot matched line\r\n{line2\r\n{line2.1\r\n",
        "^{",
        False,
        None,
        [(b"{line1\r\n{line1.1", 18), (b"not matched line", 18), (b"{line2\r\n{line2.1", 18)],
        id="not matched line: \r\n",
    ),
    pytest.param(
        b"\n",
        b"{matched line",
        "^{",
        False,
        None,
        [(b"{matched line", 14)],
        id="only matched line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{matched line",
        "^{",
        False,
        None,
        [(b"{matched line", 15)],
        id="only matched line: \r\n",
    ),
    pytest.param(
        b"\n",
        b"{line1\n{line1.1\n\n{line2\n{line2.1\n",
        "^{",
        False,
        None,
        [(b"{line1\n{line1.1", 16), (b"", 1), (b"\n{line2\n{line2.1", 16)],
        id="not matched empty line: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{line1\r\n{line1.1\r\n\r\n{line2\r\n{line2.1\r\n",
        "^{",
        False,
        None,
        [(b"{line1\r\n{line1.1", 18), (b"", 2), (b"\r\n{line2\r\n{line2.1", 18)],
        id="not matched empty line: \r\n",
    ),
    pytest.param(
        b"\n",
        b"{line1\npanic:\n~stacktrace~\n{line2\n",
        "^{",
        True,
        None,
        [(b"{line1", 7), (b"panic:\n~stacktrace~", 20), (b"{line2", 7)],
        id="negate: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{line1\r\npanic:\r\n~stacktrace~\r\n{line2\r\n",
        "^{",
        True,
        None,
        [(b"{line1", 8), (b"panic:\r\n~stacktrace~", 22), (b"{line2", 8)],
        id="negate: \r\n",
    ),
    pytest.param(
        b"\n",
        b"{line1\n{line1.1\n{line1.2\n",
        "^{",
        False,
        2,
        [(b"{line1\n{line1.1", 25)],
        id="truncated: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{line1\r\n{line1.1\r\n{line1.2\r\n",
        "^{",
        False,
        2,
        [(b"{line1\r\n{line1.1", 28)],
        id="truncated: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("newline,feed,pattern,negate,max_lines,expected_events", while_multiline_collect)
def test_while_multiline(
    newline: bytes,
    feed: bytes,
    pattern: str,
    negate: bool,
    max_lines: Optional[int],
    expected_events: list[tuple[bytes, int]],
) -> None:
    if max_lines is None:
        max_lines = 5

    while_multiline: WhileMultiline = WhileMultiline(pattern=pattern, negate=negate, max_lines=max_lines)

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    while_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in while_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


while_multiline_collect_circuitbreaker = [
    pytest.param(
        b"\n",
        b"{line1\n{line1.1\nnot matched line\n{line2\n{line2.1\n",
        [(b"{line1", 7), (b"{line1.1", 9), (b"not matched line", 17), (b"{line2", 7), (b"{line2.1", 9)],
        id="circuit breaker: \n",
    ),
    pytest.param(
        b"\r\n",
        b"{line1\r\n{line1.1\r\nnot matched line\r\n{line2\r\n{line2.1\r\n",
        [(b"{line1", 8), (b"{line1.1", 10), (b"not matched line", 18), (b"{line2", 8), (b"{line2.1", 10)],
        id="circuit breaker: \r\n",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("newline,feed,expected_events", while_multiline_collect_circuitbreaker)
@mock.patch("share.multiline.timedelta_circuit_breaker", new=datetime.timedelta(seconds=0))
def test_while_multiline_circuitbreaker(newline: bytes, feed: bytes, expected_events: list[tuple[bytes, int]]) -> None:
    while_multiline: WhileMultiline = WhileMultiline(pattern="^{")

    def feed_iterator(content: bytes) -> FeedIterator:
        for line in content.splitlines():
            yield line, newline

    while_multiline.feed = feed_iterator(feed)

    collected_events: list[tuple[bytes, int]] = []
    for event, event_length, _ in while_multiline.collect():
        collected_events.append((event, event_length))

    assert collected_events == expected_events


@pytest.mark.unit
class TestMultilineEquality(TestCase):
    def test_equality(self) -> None:
        with self.subTest("count multiline is not equal to while_pattern multiline"):
            assert not CountMultiline(count_lines=0) == WhileMultiline(pattern="pattern")

        with self.subTest("pattern multiline is not equal to count multiline"):
            assert not PatternMultiline(pattern="pattern", match="after") == CountMultiline(count_lines=0)

        with self.subTest("while_pattern multiline is not equal to pattern multiline"):
            assert not WhileMultiline(pattern="pattern") == PatternMultiline(pattern="pattern", match="after")
