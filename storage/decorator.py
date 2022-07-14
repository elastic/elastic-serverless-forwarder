# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from io import BytesIO
from typing import Any, Iterator, Optional, Union

import ujson

from share import ProtocolMultiline, shared_logger

from .storage import CHUNK_SIZE, GetByLinesCallable, ProtocolStorageType, StorageReader


# For overriding in benchmark
def json_parser(payload: bytes) -> Any:
    return ujson.loads(payload)


def multi_line(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content collected by multiline
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        ending_offset: int = range_start

        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if not multiline_processor:
            iterator = func(storage, range_start, body, is_gzipped, content_length)
            for data, json_object, original_ending_offset, original_starting_offset, newline_length in iterator:
                assert isinstance(data, bytes)

                shared_logger.debug("multi_line skipped", extra={"offset": original_ending_offset})

                yield data, json_object, original_ending_offset, original_starting_offset, newline_length
        else:

            def iterator_to_multiline_feed() -> Iterator[tuple[bytes, bytes, int]]:
                newline: bytes = b""
                previous_newline_length: int = 0
                for original_data, _, _, _, newline_length in func(
                    storage, range_start, body, is_gzipped, content_length
                ):
                    assert isinstance(original_data, bytes)

                    if newline_length != previous_newline_length:
                        previous_newline_length = newline_length
                        if newline_length == 2:
                            newline = b"\r\n"
                        elif newline_length == 1:
                            newline = b"\n"
                        else:
                            newline = b""

                    yield original_data, newline, newline_length

            multiline_processor.feed = iterator_to_multiline_feed()

            for multiline_data, multiline_ending_offset, newline_length in multiline_processor.collect():
                starting_offset = ending_offset
                ending_offset += multiline_ending_offset
                shared_logger.debug("multi_line lines", extra={"offset": ending_offset})

                yield multiline_data, None, ending_offset, starting_offset, newline_length

    return wrapper


class JsonCollector:
    """
    ProtocolStorage decorator for returning content by collected json object (if any) spanning multiple lines
    """

    def __init__(self, function: GetByLinesCallable[ProtocolStorageType]):
        self._function: GetByLinesCallable[ProtocolStorageType] = function

        self._starting_offset: int = 0
        self._ending_offset: int = 0
        self._newline_length = -1
        self._unfinished_line: bytes = b""

        self._is_a_json_object: bool = False
        self._is_a_json_object_circuit_broken: bool = True
        self._is_a_json_object_circuit_breaker: int = 0

    def _collector(
        self, data: bytes, newline: bytes, newline_length: int
    ) -> Iterator[tuple[bytes, Optional[dict[str, Any]]]]:
        try:
            # let's buffer the content
            # we receive data without newline
            # let's append it as well
            self._unfinished_line += data + newline

            # let's try to decode
            json_object = json_parser(self._unfinished_line)

            # it didn't raise: we collected a json object
            data_to_yield = self._unfinished_line

            # let's reset the buffer
            self._unfinished_line = b""

            # let's increase the offset for yielding
            self._starting_offset = self._ending_offset
            self._ending_offset += len(data_to_yield)

            # let's decrease the circuit breaker
            if newline != b"":
                self._is_a_json_object_circuit_breaker -= data_to_yield.count(newline) - 1
            else:
                self._is_a_json_object_circuit_breaker -= 1

            # let's trim leading newline
            data_to_yield = data_to_yield.strip(b"\r\n").strip(b"\n")

            # let's set the flag for json object
            self._is_a_json_object = True

            # finally yield
            yield data_to_yield, json_object
        # it raised: we didn't collect enough content to reach the end of the json object: let's keep iterating
        except ValueError:
            # it's an empty line, let's yield it
            if self._is_a_json_object and len(self._unfinished_line.strip(b"\r\n").strip(b"\n")) == 0:
                # let's reset the buffer
                self._unfinished_line = b""

                # let's increase the offset for yielding
                self._starting_offset = self._ending_offset
                self._ending_offset += newline_length

                # finally yield
                yield b"", None
            else:
                # buffer was not a complete json object
                # let's increase the circuit breaker
                self._is_a_json_object_circuit_breaker += 1

                # if the first 1k lines are not a json object let's give up
                if self._is_a_json_object_circuit_breaker > 1000:
                    self._is_a_json_object_circuit_broken = True

    @staticmethod
    def _buffer_yield(buffer: bytes) -> GetByLinesCallable[ProtocolStorageType]:
        def wrapper(
            storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
        ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:

            yield buffer, None, range_start, 0, 0

        return wrapper

    def __call__(
        self, storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if multiline_processor:
            iterator = self._function(storage, range_start, body, is_gzipped, content_length)
            for data, _, original_ending_offset, original_starting_offset, newline_length in iterator:
                assert isinstance(data, bytes)
                shared_logger.debug("JsonCollector skipped", extra={"offset": original_ending_offset})

                yield data, None, original_ending_offset, original_starting_offset, newline_length
        else:
            newline: bytes = b""

            has_an_object_start: bool = False
            wait_for_object_start: bool = True
            wait_for_object_start_buffer: bytes = b""

            self._ending_offset = range_start
            self._starting_offset = 0
            self._newline_length = -1
            self._unfinished_line = b""

            self._is_a_json_object = False
            self._is_a_json_object_circuit_broken = False
            self._is_a_json_object_circuit_breaker = 0

            iterator = self._function(storage, range_start, body, is_gzipped, content_length)
            for data, _, original_line_ending_offset, original_line_starting_offset, newline_length in iterator:
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
                            storage, range_start, body, is_gzipped, content_length  # type:ignore
                        )

                        for line, _, ending_offset, starting_offset, original_newline_length in iterator:
                            yield line, None, ending_offset, starting_offset, original_newline_length

                        # let's reset the buffer
                        wait_for_object_start_buffer = b""
                        yield data, None, original_line_ending_offset, original_line_starting_offset, newline_length
                    else:
                        # let's yield wait_for_object_start_buffer: it is newline only content
                        iterator = by_lines(self._buffer_yield(wait_for_object_start_buffer))(
                            storage, range_start, body, is_gzipped, content_length  # type:ignore
                        )

                        for line, _, ending_offset, starting_offset, original_newline_length in iterator:
                            self._starting_offset = self._ending_offset
                            self._ending_offset += ending_offset - self._ending_offset
                            yield line, None, self._ending_offset, self._starting_offset, newline_length

                        # let's reset the buffer
                        wait_for_object_start_buffer = b""

                        for data_to_yield, json_object in self._collector(data, newline, newline_length):
                            shared_logger.debug("JsonCollector objects", extra={"offset": self._ending_offset})
                            yield data_to_yield, json_object, self._ending_offset, self._starting_offset, newline_length

                        if self._is_a_json_object_circuit_broken:
                            # let's yield what we have so far
                            iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                                storage, range_start, body, is_gzipped, content_length  # type:ignore
                            )
                            for line, _, ending_offset, starting_offset, original_newline_length in iterator:
                                yield line, None, ending_offset, starting_offset, original_newline_length

                            # let's set the flag for direct yield from now on
                            has_an_object_start = False

                            # let's reset the buffer
                            self._unfinished_line = b""

            # in this case we could have a trailing new line in what's left in the buffer
            # or the content had a leading `{` but was not a json object before the circuit breaker intercepted it:
            # let's fallback to by_lines()
            if not self._is_a_json_object:
                iterator = by_lines(self._buffer_yield(self._unfinished_line))(
                    storage, range_start, body, is_gzipped, content_length  # type:ignore
                )
                for line, _, ending_offset, starting_offset, newline_length in iterator:
                    self._starting_offset = self._ending_offset
                    self._ending_offset += ending_offset - self._ending_offset

                    yield line, None, self._ending_offset, self._starting_offset, newline_length


def by_lines(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content split by lines
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        ending_offset: int = range_start
        unfinished_line: bytes = b""

        newline: bytes = b""
        newline_length: int = 0

        iterator = func(storage, range_start, body, is_gzipped, content_length)
        for data, _, line_ending_offset, line_starting_offset, _ in iterator:
            assert isinstance(data, bytes)

            unfinished_line += data
            lines = unfinished_line.decode("UTF-8").splitlines()

            if len(lines) == 0:
                continue

            if newline_length == 0:
                if unfinished_line.find(b"\r\n") > -1:
                    newline = b"\r\n"
                    newline_length = 2
                elif unfinished_line.find(b"\n") > -1:
                    newline = b"\n"
                    newline_length = 1

            if unfinished_line.endswith(newline):
                unfinished_line = lines.pop().encode() + newline
            else:
                unfinished_line = lines.pop().encode()

            for line in lines:
                line_encoded = line.encode("UTF-8")
                starting_offset = ending_offset
                ending_offset += len(line_encoded) + newline_length
                shared_logger.debug("by_line lines", extra={"offset": ending_offset})

                yield line_encoded, None, ending_offset, starting_offset, newline_length

        if len(unfinished_line) > 0:
            starting_offset = ending_offset
            ending_offset += len(unfinished_line)

            if newline_length > 0 and not unfinished_line.endswith(newline):
                newline_length = 0

            unfinished_line = unfinished_line.rstrip(newline)

            shared_logger.debug("by_line unfinished_line", extra={"offset": ending_offset})

            yield unfinished_line, None, ending_offset, starting_offset, newline_length

    return wrapper


def inflate(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning inflated content in case the original is gzipped
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        iterator = func(storage, range_start, body, is_gzipped, content_length)
        for data, _, line_ending_offset, line_starting_offset, newline_length in iterator:
            if is_gzipped:
                gzip_stream = gzip.GzipFile(fileobj=data)  # type:ignore
                gzip_stream.seek(range_start)
                while True:
                    inflated_chunk: bytes = gzip_stream.read(CHUNK_SIZE)
                    if len(inflated_chunk) == 0:
                        break

                    buffer: BytesIO = BytesIO()
                    buffer.write(inflated_chunk)

                    shared_logger.debug("inflate inflate", extra={"offset": line_ending_offset})
                    yield buffer.getvalue(), None, line_ending_offset, line_starting_offset, 0
            else:
                shared_logger.debug("inflate plain", extra={"offset": line_ending_offset})
                yield data, None, line_ending_offset, line_starting_offset, 0

    return wrapper
