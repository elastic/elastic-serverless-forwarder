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

        self._starting_offset: int = 0
        self._ending_offset: int = 0
        self._offset_skew: int = 0
        self._newline_length = -1
        self._unfinished_line: bytes = b""

        self._is_a_json_object: bool = False
        self._is_a_json_object_circuit_broken: bool = True
        self._is_a_json_object_circuit_breaker: int = 0

    def _collector(self, data: bytes, newline: bytes) -> Iterator[bytes]:
        try:
            # let's buffer the content
            # we receive data without newline
            # let's append it as well
            self._unfinished_line += data + newline

            # let's try to decode
            json_library.loads(self._unfinished_line)

            # it didn't raise: we collected a json object
            data_to_yield = self._unfinished_line

            # let's reset the buffer
            self._unfinished_line = b""

            # let's increase the offset for yielding
            self._starting_offset = self._ending_offset
            self._ending_offset += len(data_to_yield) + self._offset_skew

            # let's reset the offset skew
            self._offset_skew = 0

            # let's decrease the circuit breaker
            if newline != b"":
                self._is_a_json_object_circuit_breaker -= data_to_yield.count(newline) - 1
            else:
                self._is_a_json_object_circuit_breaker -= 1

            # let's trim leading newline
            data_to_yield = data_to_yield.strip(newline)

            # let's set the flag for json object
            self._is_a_json_object = True

            # finally yield
            yield data_to_yield
        # it raised: we didn't collect enough content to reach the end of the json object: let's keep iterating
        except ValueError:
            # buffer was not a complete json object
            # let's increase the circuit breaker
            self._is_a_json_object_circuit_breaker += 1

            # if the first 1k lines are not a json object let's give up
            if self._is_a_json_object_circuit_breaker > 1000:
                self._is_a_json_object_circuit_broken = True

    @staticmethod
    def _buffer_yield(buffer: bytes) -> GetByLinesCallable[CommonStorageType]:
        def wrapper(
            storage: CommonStorageType, range_start: int, body: BytesIO, content_type: str, content_length: int
        ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, int]]:

            yield buffer, range_start, 0, 0

        return wrapper

    def __call__(
        self, storage: CommonStorageType, range_start: int, body: BytesIO, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, int]]:
        newline: bytes = b""

        has_an_object_start: bool = False
        wait_for_object_start: bool = True
        wait_for_object_start_buffer: bytes = b""

        self._ending_offset = range_start
        self._starting_offset = 0
        self._offset_skew = 0
        self._newline_length = -1
        self._unfinished_line = b""

        self._is_a_json_object = False
        self._is_a_json_object_circuit_broken = False
        self._is_a_json_object_circuit_breaker = 0

        iterator = self._function(storage, range_start, body, content_type, content_length)
        for data, original_line_ending_offset, original_line_starting_offset, newline_length in iterator:
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

            if not wait_for_object_start:
                # if it's not a json object we can just forward the content by lines
                if not has_an_object_start:
                    # let's consume the buffer we set for waiting for object_start before the circuit breaker
                    iterator = by_lines(self._buffer_yield(wait_for_object_start_buffer))(
                        storage, range_start, body, content_type, content_length  # type:ignore
                    )

                    for line, ending_offset, starting_offset, original_newline_length in iterator:
                        yield line, ending_offset, starting_offset, original_newline_length

                    # let's reset the buffer
                    wait_for_object_start_buffer = b""
                    yield data, original_line_ending_offset, original_line_starting_offset, newline_length
                else:
                    # let's balance the offset skew, wait_for_object_start_buffer is newline only content
                    self._offset_skew += len(wait_for_object_start_buffer)

                    # let's reset the buffer
                    wait_for_object_start_buffer = b""

                    for data_to_yield in self._collector(data, newline):
                        shared_logger.debug("JsonCollector objects", extra={"offset": self._ending_offset})
                        yield data_to_yield, self._ending_offset, self._starting_offset, newline_length

                    if self._is_a_json_object_circuit_broken:
                        # let's yield what we have so far
                        iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                            storage, range_start, body, content_type, content_length  # type:ignore
                        )
                        for line, ending_offset, starting_offset, original_newline_length in iterator:
                            yield line, ending_offset, starting_offset, original_newline_length

                        # let's set the flag for direct yield from now on
                        has_an_object_start = False

                        # let's reset the buffer
                        self._unfinished_line = b""

        # in this case we could have a trailing new line in what's left in the buffer
        # or the content had a leading `{` but was not a json object before the circuit breaker intercepted it:
        # let's fallback to by_lines()
        if not self._is_a_json_object:
            iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                storage, range_start, body, content_type, content_length  # type:ignore
            )
            for line, ending_offset, starting_offset, newline_length in iterator:
                self._starting_offset = self._ending_offset
                self._ending_offset += ending_offset - self._ending_offset

                yield line, self._ending_offset, self._starting_offset, newline_length

    def __get__(
        self, storage: CommonStorageType, owner: Any
    ) -> partial[Iterator[tuple[Union[StorageReader, bytes], int, int, int]]]:
        return partial(self, storage)


def by_lines(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    """
    CommonStorageType decorator for returning content split by lines
    """

    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, int]]:
        ending_offset: int = range_start
        unfinished_line: bytes = b""

        newline: bytes = b""
        newline_length: int = 0

        iterator = func(storage, range_start, body, content_type, content_length)
        for data, line_ending_offset, line_starting_offset, _newline_length in iterator:
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
                starting_offset = ending_offset
                ending_offset += len(line_encoded) + newline_length
                shared_logger.debug("by_line lines", extra={"offset": ending_offset})

                yield line_encoded, ending_offset, starting_offset, newline_length

        if len(unfinished_line) > 0:
            starting_offset = ending_offset
            ending_offset += len(unfinished_line)

            if newline_length > 0 and not unfinished_line.endswith(newline):
                newline_length = 0

            unfinished_line = unfinished_line.rstrip(newline)

            shared_logger.debug("by_line unfinished_line", extra={"offset": ending_offset})

            yield unfinished_line, ending_offset, starting_offset, newline_length

    return wrapper


def inflate(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    """
    CommonStorageType decorator for returning inflated content in case the original is gzipped
    """

    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, int]]:
        iterator = func(storage, range_start, body, content_type, content_length)
        for data, line_ending_offset, line_starting_offset, newline_length in iterator:
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
                    yield buffer.getvalue(), line_ending_offset, line_starting_offset, 0
            else:
                shared_logger.debug("inflate plain", extra={"offset": line_ending_offset})
                yield data, line_ending_offset, line_starting_offset, 0

    return wrapper
