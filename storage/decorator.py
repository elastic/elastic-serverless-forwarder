# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from functools import partial
from io import BytesIO
from typing import Any, Iterator, Union

import ujson

from share import shared_logger

from .storage import CHUNK_SIZE, CommonStorageType, GetByLinesCallable, StorageReader

# For overriding in benchmark
json_library = ujson


class JsonCollector:
    """
    CommonStorageType decorator for returning content by collected json object (if any) spanning multiple lines
    """

    def __init__(self, function: GetByLinesCallable[CommonStorageType]):
        self._function: GetByLinesCallable[CommonStorageType] = function

        self._offset: int = 0
        self._newline_length = -1
        self._unfinished_line: bytes = b""

        self._is_a_json_object: bool = True
        self._is_a_json_object_circuit_breaker: int = 0

    def _collector(self, data: bytes, newline: bytes) -> Iterator[bytes]:
        try:
            # let's buffer the content
            # we receive data without newline
            # let's append it as well
            self._unfinished_line += data + newline

            # let's try to deserialize only until the circuit breaker will kick in
            if self._is_a_json_object:
                # let's try to decode
                json_library.loads(self._unfinished_line)

                # it didn't raise: we collected a json object
                data_to_yield = self._unfinished_line
                # let's reset the buffer
                self._unfinished_line = b""

                # let's increase the offset for yielding (
                self._offset += len(data_to_yield)

                # let's decrease the circuit breaker
                if newline != b"":
                    self._is_a_json_object_circuit_breaker -= data_to_yield.count(newline) - 1
                else:
                    self._is_a_json_object_circuit_breaker -= 1

                # let's trim leading newline
                data_to_yield = data_to_yield.lstrip(newline)

                # finally yield
                yield data_to_yield
        # it raised: we didn't collect enough content to reach the end of the json object: let's keep iterating
        except ValueError:
            # buffer was not a complete json object
            # let's increase the circuit breaker
            self._is_a_json_object_circuit_breaker += 1

            # if the first 1k lines are not a json object let's give up
            if self._is_a_json_object_circuit_breaker > 1000:
                self._is_a_json_object = False

    @staticmethod
    def _buffer_yield(buffer: bytes) -> GetByLinesCallable[CommonStorageType]:
        def wrapper(
            storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
        ) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:

            if len(buffer) > 0:
                yield buffer, range_start, 0

        return wrapper

    def __call__(
        self, storage: CommonStorageType, range_start: int, body: BytesIO, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:
        newline: bytes = b""

        has_an_object_start: bool = False
        wait_for_object_start: bool = True
        wait_for_object_start_buffer: bytes = b""
        wait_for_object_start_buffer_circuit_breaker: int = 0

        self._offset = range_start
        self._newline_length = -1
        self._unfinished_line = b""

        self._is_a_json_object = True
        self._is_a_json_object_circuit_breaker = 0

        iterator = self._function(storage, range_start, body, content_type, content_length)
        for data, original_line_ending_offset, newline_length in iterator:
            assert isinstance(data, bytes)

            if newline_length != self._newline_length:
                self._newline_length = newline_length
                if newline_length == 2:
                    newline = b"\r\n"
                elif newline_length == 1:
                    newline = b"\n"
                else:
                    newline = b""

            # let's check until we got some content from split by lines
            if wait_for_object_start:
                # let's increase the circuit breaker counter
                wait_for_object_start_buffer_circuit_breaker += 1

                # we check for a potential json object on
                # we strip leading space to be safe on padding
                stripped_data = data.decode("utf-8").lstrip()

                # if range_start is greater than zero, data can be empty
                if len(stripped_data) > 0:
                    wait_for_object_start = False
                    if stripped_data[0] == "{":
                        # we mark the potentiality of a json object start
                        # CAVEAT: if the log entry starts with `{` but the
                        # content is not json, we buffer the first 10k lines
                        # before the circuit breaker kicks in
                        has_an_object_start = True
                else:
                    # let's buffer discarded data including newline for eventual by_lines() fallback
                    wait_for_object_start_buffer += data + newline

                # let's wait no longer than 1k lines
                if wait_for_object_start_buffer_circuit_breaker > 1000:
                    wait_for_object_start = False

            if not wait_for_object_start:
                # if it's not a json object we can just forward the content by lines
                if not has_an_object_start:
                    # let's consume the buffer we set for waiting for object_start before the circuit breaker
                    if len(wait_for_object_start_buffer) > 0:
                        iterator = by_lines(self._buffer_yield(wait_for_object_start_buffer))(
                            storage, range_start, body, content_type, content_length # type:ignore
                        )

                        for line, offset, original_newline_length in iterator:
                            yield line, offset, original_newline_length

                    # let's reset the buffer
                    wait_for_object_start_buffer = b""
                    yield data, original_line_ending_offset, newline_length
                else:
                    buffer_length = len(wait_for_object_start_buffer)
                    total_newline_length = wait_for_object_start_buffer.count(newline) * newline_length
                    # let's consume the buffer we set for waiting for object_start before the circuit breaker
                    # only if it does contain more than newlines only
                    if buffer_length > 0 and buffer_length != total_newline_length:
                        iterator = by_lines(self._buffer_yield(wait_for_object_start_buffer))(
                            storage, range_start, body, content_type, content_length # type:ignore
                        )

                        # we still can have json like object
                        json_object_was_yield = False
                        for line, offset, original_newline_length in iterator:
                            assert isinstance(line, bytes)

                            for data_to_yield in self._collector(line, newline):
                                shared_logger.debug("JsonCollector objects", extra={"offset": self._offset})
                                json_object_was_yield = True
                                yield data_to_yield, self._offset, newline_length

                            # let's yield if it's not a json
                            if not json_object_was_yield:
                                yield line, offset, original_newline_length
                    else:
                        # let's balance the offset for newline only content
                        if newline_length > 0:
                            self._offset += total_newline_length

                    # let's reset the buffer
                    wait_for_object_start_buffer = b""

                    for data_to_yield in self._collector(data, newline):
                        shared_logger.debug("JsonCollector objects", extra={"offset": self._offset})
                        yield data_to_yield, self._offset, newline_length

                    if not self._is_a_json_object:
                        # let's yield what we have so far
                        iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                            storage, range_start, body, content_type, content_length # type:ignore
                        )
                        for line, offset, original_newline_length in iterator:
                            yield line, offset, original_newline_length

                        # let's set the flag for direct yield from now on
                        has_an_object_start = False

                        # let's reset the buffer
                        self._unfinished_line = b""

        # either we have a trailing new line in what's left in the buffer
        # or the content had a leading `{` but was not a json object
        # before the circuit breaker intercepted it:
        # let's fallback to by_lines()
        self._unfinished_line = self._unfinished_line.strip(b"\n").strip(b"\r\n")
        if len(self._unfinished_line) > 0:
            iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                storage, range_start, body, content_type, content_length # type:ignore
            )
            for line, offset, newline_length in iterator:
                self._offset += offset - self._offset

                yield line, self._offset, newline_length

    def __get__(
        self, storage: CommonStorageType, owner: Any
    ) -> partial[Iterator[tuple[Union[StorageReader, bytes], int, int]]]:
        return partial(self, storage)


def by_lines(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    """
    CommonStorageType decorator for returning content split by lines
    """

    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:
        offset: int = range_start
        unfinished_line: bytes = b""

        newline: bytes = b""
        newline_length: int = 0

        iterator = func(storage, range_start, body, content_type, content_length)
        for data, line_ending_offset, _newline_length in iterator:
            assert isinstance(data, bytes)

            unfinished_line += data
            lines = unfinished_line.decode("UTF-8").splitlines()

            if len(lines) == 0:
                continue

            if newline_length == 0:
                if unfinished_line.find(b"\r\n") > -1:
                    newline = b"\r\n"
                    newline_length = len(newline)
                elif unfinished_line.find(b"\n") > -1:
                    newline = b"\n"
                    newline_length = len(newline)

            if unfinished_line.endswith(newline):
                unfinished_line = lines.pop().encode() + newline
            else:
                unfinished_line = lines.pop().encode()

            for line in lines:
                line_encoded = line.encode("UTF-8")
                offset += len(line_encoded) + newline_length
                shared_logger.debug("by_line lines", extra={"offset": offset})

                yield line_encoded, offset, newline_length

        if len(unfinished_line) > 0:
            offset += len(unfinished_line)

            if newline_length > 0 and not unfinished_line.endswith(newline):
                newline_length = 0

            unfinished_line = unfinished_line.rstrip(newline)

            shared_logger.debug("by_line unfinished_line", extra={"offset": offset})

            yield unfinished_line, offset, newline_length

    return wrapper


def inflate(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    """
    CommonStorageType decorator for returning inflated content in case the original is gzipped
    """

    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:
        iterator = func(storage, range_start, body, content_type, content_length)
        for data, line_ending_offset, newline_length in iterator:
            if content_type == "application/x-gzip":
                gzip_stream = gzip.GzipFile(fileobj=data)  # type:ignore
                gzip_stream.seek(range_start)
                while True:
                    inflated_chunk: bytes = gzip_stream.read(CHUNK_SIZE)
                    if len(inflated_chunk) == 0:
                        break

                    buffer: BytesIO = BytesIO()
                    buffer.write(inflated_chunk)

                    shared_logger.debug("inflate inflate", extra={"offset": line_ending_offset})
                    yield buffer.getvalue(), line_ending_offset, 0
            else:
                shared_logger.debug("inflate plain", extra={"offset": line_ending_offset})
                yield data, line_ending_offset, 0

    return wrapper
