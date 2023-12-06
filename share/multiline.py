# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.


# This file is a porting of the multiline processor on beats.

from __future__ import annotations

import datetime
import re
from abc import ABCMeta
from typing import Callable, Iterator, Optional, Protocol

from typing_extensions import TypeAlias

default_max_bytes: int = 10485760  # Default maximum number of bytes to return in one multi-line event
default_max_lines: int = 500  # Default maximum number of lines to return in one multi-line event
default_multiline_timeout: int = 5  # Default timeout in secs to finish a multi-line event.

timedelta_circuit_breaker: datetime.timedelta = datetime.timedelta(seconds=5)

# CollectTuple is a tuple representing the multilines bytes content, the length of the content and the newline found
# These data is instrumental to the `StorageDecoratorIterator` that needs the content to yield, the starting and ending
# offsets and the newline
CollectTuple: TypeAlias = tuple[bytes, int, bytes]

# CollectIterator yields a `CollectTuple`
CollectIterator: TypeAlias = Iterator[CollectTuple]

# FeedIterator yields a tuple representing the content and its newline to feed of the `ProtocolMultiline` implementation
FeedIterator: TypeAlias = Iterator[tuple[bytes, bytes]]


class CommonMultiline(metaclass=ABCMeta):
    """
    Common class for Multiline components
    """

    _feed: FeedIterator
    _buffer: CollectBuffer

    _pre_collect_buffer: bool

    @property
    def feed(self) -> FeedIterator:
        return self._feed

    @feed.setter
    def feed(self, value: FeedIterator) -> None:
        self._feed = value


class ProtocolMultiline(Protocol):
    """
    Protocol class for Multiline components
    """

    _feed: FeedIterator
    _buffer: CollectBuffer

    @property
    def feed(self) -> FeedIterator:
        pass  # pragma: no cover

    @feed.setter
    def feed(self, value: FeedIterator) -> None:
        pass  # pragma: no cover

    def collect(self) -> CollectIterator:
        pass  # pragma: no cover


class CollectBuffer:
    """
    MessageBuffer.
    This class implements a buffer for collecting multiline content with criteria.
    """

    def __init__(self, max_bytes: int, max_lines: int, skip_newline: bool):
        self._max_bytes: int = max_bytes
        self._max_lines: int = max_lines
        self._skip_newline: bool = skip_newline

        self._buffer: bytes = b""
        self._previous: bytes = b""
        self._previous_newline: bytes = b""
        self._previous_was_empty: bool = False
        self._buffer_lines: int = 0
        self._processed_lines: int = 0
        self._current_length: int = 0

    def collect_and_reset(self) -> CollectTuple:
        data = self._buffer
        current_length = self._current_length

        self._buffer = b""
        self._current_length = 0
        self._buffer_lines = 0
        self._processed_lines = 0

        self.previous = b""

        if data.find(b"\r\n") > -1:
            newline = b"\r\n"
        elif data.find(b"\n") > -1:
            newline = b"\n"
        else:
            newline = b""

        return data, current_length, newline

    def is_empty(self) -> bool:
        return self._buffer_lines == 0

    @property
    def previous(self) -> bytes:
        return self._previous

    @previous.setter
    def previous(self, value: bytes) -> None:
        self._previous = value

    def grow(self, data: bytes, newline: bytes) -> None:
        add_newline: bool = False
        if (len(self._buffer) > 0 or self._previous_was_empty) and not self._skip_newline:
            self._previous_was_empty = False
            add_newline = True

        if len(data) == 0:
            self._previous_was_empty = True

        below_max_lines: bool = False
        if self._max_lines <= 0 or self._buffer_lines < self._max_lines:
            below_max_lines = True

        grow_size: int = self._max_bytes - len(self._buffer)

        below_max_bytes: bool = False
        if self._max_bytes <= 0 or grow_size > 0:
            below_max_bytes = True

        self._current_length += len(data) + len(newline)

        if below_max_lines and below_max_bytes:
            if grow_size < 0 or grow_size > len(data):
                grow_size = len(data)

            if add_newline:
                self._buffer += self._previous_newline

            self._buffer += data[:grow_size]
            self._buffer_lines += 1

            self._previous_newline = newline

        self.previous = data

        self._processed_lines += 1


class CountMultiline(CommonMultiline):
    """
    CountMultiline Multiline.
    This class implements concrete Count Multiline.
    """

    def __init__(
        self,
        count_lines: int,
        max_bytes: int = default_max_bytes,
        max_lines: int = default_max_lines,
        skip_newline: bool = False,
    ):
        self._max_bytes: int = max_bytes
        self._max_lines: int = max_lines
        self._skip_newline: bool = skip_newline

        self._count_lines: int = count_lines

        self._current_count: int = 0
        self._buffer: CollectBuffer = CollectBuffer(max_bytes, max_lines, skip_newline)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CountMultiline):
            return False

        return (
            self._count_lines == other._count_lines
            and self._max_bytes == other._max_bytes
            and self._max_lines == self._max_lines
            and self._skip_newline == self._skip_newline
        )

    def collect(self) -> CollectIterator:
        last_iteration_datetime: datetime.datetime = datetime.datetime.utcnow()
        for data, newline in self.feed:
            self._buffer.grow(data, newline)
            self._current_count += 1
            if (
                self._count_lines == self._current_count
                or (datetime.datetime.utcnow() - last_iteration_datetime) > timedelta_circuit_breaker
            ):
                self._current_count = 0
                yield self._buffer.collect_and_reset()

        if not self._buffer.is_empty():
            yield self._buffer.collect_and_reset()


# WhileMatcherCallable accepts a pattern in bytes to be compiled as regex.
# It returns a boolean indicating if the content matches a "while" multiline pattern or not.
WhileMatcherCallable = Callable[[bytes], bool]


class WhileMultiline(CommonMultiline):
    """
    WhileMultiline Multiline.
    This class implements concrete While Multiline.
    """

    def __init__(
        self,
        pattern: str,
        negate: bool = False,
        max_bytes: int = default_max_bytes,
        max_lines: int = default_max_lines,
        skip_newline: bool = False,
    ):
        self._pattern: str = pattern
        self._negate: bool = negate
        self._max_bytes: int = max_bytes
        self._max_lines: int = max_lines

        self._skip_newline: bool = skip_newline

        self._matcher: WhileMatcherCallable = self._setup_pattern_matcher(pattern, negate)

        self._buffer: CollectBuffer = CollectBuffer(max_bytes, max_lines, skip_newline)

        self._pre_collect_buffer = True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WhileMultiline):
            return False

        return (
            self._pattern == other._pattern
            and self._negate == other._negate
            and self._max_bytes == other._max_bytes
            and self._max_lines == self._max_lines
            and self._skip_newline == self._skip_newline
        )

    def _setup_pattern_matcher(self, pattern: str, negate: bool) -> WhileMatcherCallable:
        re_pattern: re.Pattern[bytes] = re.compile(pattern.encode("utf-8"))

        matcher: WhileMatcherCallable = self._get_pattern_matcher(re_pattern)
        if negate:
            matcher = self._negated_matcher(matcher)

        return matcher

    @staticmethod
    def _get_pattern_matcher(pattern: re.Pattern[bytes]) -> WhileMatcherCallable:
        def match(line: bytes) -> bool:
            return pattern.search(line) is not None

        return match

    @staticmethod
    def _negated_matcher(matcher: WhileMatcherCallable) -> WhileMatcherCallable:
        def negate(line: bytes) -> bool:
            return not matcher(line)

        return negate

    def collect(self) -> CollectIterator:
        last_iteration_datetime: datetime.datetime = datetime.datetime.utcnow()
        for data, newline in self.feed:
            if not self._matcher(data):
                if self._buffer.is_empty():
                    self._buffer.grow(data, newline)
                    yield self._buffer.collect_and_reset()
                else:
                    content, current_length, _ = self._buffer.collect_and_reset()
                    self._buffer.grow(data, newline)

                    yield content, current_length, newline

                    content, current_length, _ = self._buffer.collect_and_reset()

                    yield content, current_length, newline
            else:
                self._buffer.grow(data, newline)
                # no pre collect buffer in while multiline, let's check the circuit breaker after at least one grow
                if (datetime.datetime.utcnow() - last_iteration_datetime) > timedelta_circuit_breaker:
                    yield self._buffer.collect_and_reset()

        if not self._buffer.is_empty():
            yield self._buffer.collect_and_reset()


# WhileMatcherCallable accepts the previous and the current content as arguments.
# It returns a boolean indicating if the content matches a "pattern" multiline pattern or not.
PatternMatcherCallable = Callable[[bytes, bytes], bool]

# SelectCallable accepts the previous and the current content as arguments.
# It returns either the previous or current content according to the matching type ("before" or "after").
SelectCallable = Callable[[bytes, bytes], bytes]


class PatternMultiline(CommonMultiline):
    """
    PatternMultiline Multiline.
    This class implements concrete Pattern Multiline.
    """

    def __init__(
        self,
        pattern: str,
        match: str,
        negate: bool = False,
        flush_pattern: str = "",
        max_bytes: int = default_max_bytes,
        max_lines: int = default_max_lines,
        skip_newline: bool = False,
    ):
        self._pattern: str = pattern
        self._match: str = match
        self._negate: bool = negate

        self._max_bytes: int = max_bytes
        self._max_lines: int = max_lines
        self._skip_newline: bool = skip_newline

        self._matcher: PatternMatcherCallable = self._setup_pattern_matcher(pattern, match, negate)

        if flush_pattern:
            self._flush_pattern: Optional[re.Pattern[bytes]] = re.compile(flush_pattern.encode("utf-8"))
        else:
            self._flush_pattern = None

        self._buffer: CollectBuffer = CollectBuffer(max_bytes, max_lines, skip_newline)
        self._pre_collect_buffer: bool = True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PatternMultiline):
            return False
        return (
            self._pattern == other._pattern
            and self._match == other._match
            and self._negate == other._negate
            and self._max_bytes == other._max_bytes
            and self._max_lines == self._max_lines
            and self._skip_newline == self._skip_newline
            and self._flush_pattern == self._flush_pattern
        )

    def _setup_pattern_matcher(self, pattern: str, match: str, negate: bool) -> PatternMatcherCallable:
        re_pattern: re.Pattern[bytes] = re.compile(pattern.encode("utf-8"))

        selector: Optional[SelectCallable] = None
        if match == "before":
            selector = self._before_matcher
        else:
            selector = self._after_matcher

        assert selector is not None

        matcher: PatternMatcherCallable = self._get_pattern_matcher(re_pattern, selector)
        if negate:
            matcher = self._negated_matcher(matcher)

        return matcher

    @staticmethod
    def _get_pattern_matcher(pattern: re.Pattern[bytes], selector: SelectCallable) -> PatternMatcherCallable:
        def match(previous: bytes, current: bytes) -> bool:
            line: bytes = selector(previous, current)
            return pattern.search(line) is not None

        return match

    @staticmethod
    def _before_matcher(previous: bytes, _: bytes) -> bytes:
        return previous

    @staticmethod
    def _after_matcher(_: bytes, current: bytes) -> bytes:
        return current

    @staticmethod
    def _negated_matcher(matcher: PatternMatcherCallable) -> PatternMatcherCallable:
        def negate(previous: bytes, current: bytes) -> bool:
            return not matcher(previous, current)

        return negate

    def _check_matcher(self) -> bool:
        return (self._match == "after" and len(self._buffer.previous) > 0) or self._match == "before"

    def collect(self) -> CollectIterator:
        for data, newline in self.feed:
            last_iteration_datetime: datetime.datetime = datetime.datetime.utcnow()
            if self._pre_collect_buffer:
                self._buffer.collect_and_reset()
                self._buffer.grow(data, newline)
                self._pre_collect_buffer = False
            elif self._flush_pattern and self._flush_pattern.search(data) is not None:
                self._buffer.grow(data, newline)
                self._pre_collect_buffer = True

                yield self._buffer.collect_and_reset()
            elif (
                not self._buffer.is_empty() and self._check_matcher() and not self._matcher(self._buffer.previous, data)
            ):
                content, current_length, _ = self._buffer.collect_and_reset()

                self._buffer.grow(data, newline)

                yield content, current_length, newline
            else:
                if (datetime.datetime.utcnow() - last_iteration_datetime) > timedelta_circuit_breaker:
                    yield self._buffer.collect_and_reset()
                self._buffer.grow(data, newline)

        if not self._buffer.is_empty():
            yield self._buffer.collect_and_reset()
