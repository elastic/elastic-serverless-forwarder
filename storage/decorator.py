# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from io import BytesIO
from typing import Any, Iterator, Optional, Union

from share import ExpandEventListFromField, ProtocolMultiline, json_parser, shared_logger

from .storage import CHUNK_SIZE, GetByLinesCallable, ProtocolStorageType, StorageReader


def by_lines(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content split by lines
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
        ending_offset: int = range_start
        unfinished_line: bytes = b""

        iterator = func(storage, range_start, body, is_gzipped)

        for data, _, _, _, _ in iterator:
            assert isinstance(data, bytes)

            unfinished_line += data
            lines = unfinished_line.decode("utf-8").splitlines()

            if len(lines) == 0:
                continue

            if unfinished_line.find(b"\r\n") > -1:
                newline = b"\r\n"
            elif unfinished_line.find(b"\n") > -1:
                newline = b"\n"
            else:
                newline = b""

            # replace unfinished_line with the last element removed from lines, trailing with newline
            if unfinished_line.endswith(newline):
                unfinished_line = lines.pop().encode() + newline
            else:
                unfinished_line = lines.pop().encode()

            for line in lines:
                line_encoded = line.encode("utf-8")
                starting_offset = ending_offset
                ending_offset += len(line_encoded) + len(newline)
                shared_logger.debug("by_line lines", extra={"offset": ending_offset})

                yield line_encoded, starting_offset, ending_offset, newline, None

        if len(unfinished_line) > 0:
            if unfinished_line.endswith(b"\r\n"):
                newline = b"\r\n"
            elif unfinished_line.endswith(b"\n"):
                newline = b"\n"
            else:
                newline = b""

            unfinished_line = unfinished_line.rstrip(newline)

            starting_offset = ending_offset
            ending_offset += len(unfinished_line) + len(newline)

            shared_logger.debug("by_line unfinished_line", extra={"offset": ending_offset})

            yield unfinished_line, starting_offset, ending_offset, newline, None

    return wrapper


def multi_line(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content collected by multiline
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if not multiline_processor:
            iterator = func(storage, range_start, body, is_gzipped)
            for data, starting_offset, ending_offset, newline, event_expanded_offset in iterator:
                assert isinstance(data, bytes)

                shared_logger.debug("multi_line skipped", extra={"offset": ending_offset})

                yield data, starting_offset, ending_offset, newline, event_expanded_offset
        else:
            ending_offset = range_start

            def iterator_to_multiline_feed() -> Iterator[tuple[bytes, bytes]]:
                for data, _, _, newline, _ in func(storage, range_start, body, is_gzipped):
                    assert isinstance(data, bytes)
                    yield data, newline

            multiline_processor.feed = iterator_to_multiline_feed()

            for multiline_data, multiline_ending_offset, newline in multiline_processor.collect():
                starting_offset = ending_offset
                ending_offset += multiline_ending_offset
                shared_logger.debug("multi_line lines", extra={"offset": ending_offset})

                yield multiline_data, starting_offset, ending_offset, newline, None

    return wrapper


class JsonCollector:
    """
    ProtocolStorage decorator for returning content by collected json object (if any) spanning multiple lines
    """

    def __init__(self, function: GetByLinesCallable[ProtocolStorageType]):
        self._function: GetByLinesCallable[ProtocolStorageType] = function

        self._starting_offset: int = 0
        self._ending_offset: int = 0

        self._single_payload: bytes = b""
        self._unfinished_line: bytes = b""

        self._is_a_json_object: bool = False
        self._is_a_json_object_circuit_broken: bool = True
        self._is_a_json_object_circuit_breaker: int = 0

        self._storage: Optional[ProtocolStorageType] = None

    def _collector(self, data: bytes, newline: bytes) -> Iterator[tuple[bytes, Optional[dict[str, Any]]]]:
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
            self._handle_offset(len(data_to_yield))

            # let's decrease the circuit breaker by the number of lines in the data to yield
            if newline != b"":
                self._is_a_json_object_circuit_breaker -= data_to_yield.count(newline) - 1
            else:
                self._is_a_json_object_circuit_breaker -= 1

            # let's trim surrounding newline
            data_to_yield = data_to_yield.strip(b"\r\n").strip(b"\n")

            # let's set the flag for json object
            self._is_a_json_object = True

            # finally yield
            yield data_to_yield, json_object
        # it raised ValueError: we didn't collect enough content
        # to reach the end of the json object
        # let's keep iterating
        except ValueError:
            # it's an empty line, let's yield it
            if self._is_a_json_object and len(self._unfinished_line.strip(b"\r\n").strip(b"\n")) == 0:
                # let's reset the buffer
                self._unfinished_line = b""

                # let's increase the offset for yielding
                self._handle_offset(len(newline))

                # finally yield
                yield b"", None
            else:
                # buffer was not a complete json object
                # let's increase the circuit breaker
                self._is_a_json_object_circuit_breaker += 1

                # if the first 1k lines are not a json object let's give up
                if self._is_a_json_object_circuit_breaker > 1000:
                    self._is_a_json_object_circuit_broken = True

    def _by_lines_fallback(
        self, buffer: bytes
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
        assert self._storage is not None

        @by_lines
        def wrapper(
            storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
        ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
            data_to_yield: bytes = body.read()
            yield data_to_yield, 0, range_start, b"", None

        for line, starting_offset, ending_offset, newline, _ in wrapper(
            self._storage, self._ending_offset, BytesIO(buffer), False
        ):
            assert isinstance(line, bytes)
            yield line, starting_offset, ending_offset, newline, None

    def _handle_offset(self, offset_skew: int) -> None:
        self._starting_offset = self._ending_offset
        self._ending_offset += offset_skew

    def __call__(
        self, storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
        self._storage = storage
        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if storage.json_content_type == "disabled" or multiline_processor:
            iterator = self._function(storage, range_start, body, is_gzipped)
            for data, starting_offset, ending_offset, newline, _ in iterator:
                assert isinstance(data, bytes)
                shared_logger.debug("JsonCollector skipped", extra={"offset": ending_offset})

                yield data, starting_offset, ending_offset, newline, None
        else:
            event_list_from_field_expander: Optional[ExpandEventListFromField] = storage.event_list_from_field_expander

            has_an_object_start: bool = False
            wait_for_object_start: bool = True
            wait_for_object_start_buffer: bytes = b""

            self._ending_offset = range_start
            self._starting_offset = 0

            self._single_payload = b""
            self._unfinished_line = b""

            self._is_a_json_object = False
            self._is_a_json_object_circuit_broken = False
            self._is_a_json_object_circuit_breaker = 0

            iterator = self._function(storage, range_start, body, is_gzipped)
            # if we know it's a single json we replace body with the whole content,
            # and mark the object as started
            if storage.json_content_type == "single" and event_list_from_field_expander is None:
                single: list[tuple[Union[StorageReader, bytes], int, int, bytes]] = list(
                    [
                        (data, starting_offset, ending_offset, newline)
                        for data, starting_offset, ending_offset, newline, _ in iterator
                    ]
                )

                newline = single[0][-1]
                starting_offset = single[0][1]
                ending_offset = single[-1][2]

                data_to_yield: bytes = newline.join([x[0] for x in single])
                yield data_to_yield, starting_offset, ending_offset, newline, None
            else:
                for data, starting_offset, ending_offset, newline, _ in iterator:
                    assert isinstance(data, bytes)

                    # we still wait for the object to start
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
                            if len(wait_for_object_start_buffer) > 0:
                                # let's yield the buffer we set for waiting for object_start before the circuit breaker
                                for line, _, _, original_newline, _ in self._by_lines_fallback(
                                    wait_for_object_start_buffer
                                ):
                                    self._handle_offset(len(line) + len(original_newline))
                                    yield line, self._starting_offset, self._ending_offset, original_newline, None

                            # let's reset the buffer
                            wait_for_object_start_buffer = b""

                            yield data, starting_offset, ending_offset, newline, None
                        else:
                            # let's yield wait_for_object_start_buffer: it is newline only content
                            for line, _, _, original_newline, _ in self._by_lines_fallback(
                                wait_for_object_start_buffer
                            ):
                                self._handle_offset(len(line) + len(original_newline))
                                yield line, self._starting_offset, self._ending_offset, original_newline, None

                            # let's reset the buffer
                            wait_for_object_start_buffer = b""

                            # let's parse the data only if it is not single, or we have a field expander
                            if storage.json_content_type != "single" or event_list_from_field_expander is not None:
                                for data_to_yield, json_object in self._collector(data, newline):
                                    shared_logger.debug("JsonCollector objects", extra={"offset": self._ending_offset})
                                    if event_list_from_field_expander is not None:
                                        for (
                                            expanded_log_event,
                                            expanded_starting_offset,
                                            expanded_ending_offset,
                                            expanded_event_n,
                                        ) in event_list_from_field_expander.expand(
                                            data_to_yield, json_object, self._starting_offset, self._ending_offset
                                        ):
                                            yield (
                                                expanded_log_event,
                                                expanded_starting_offset,
                                                expanded_ending_offset,
                                                newline,
                                                expanded_event_n,
                                            )
                                    else:
                                        yield data_to_yield, self._starting_offset, self._ending_offset, newline, None

                                    del json_object
                            else:
                                self._handle_offset(len(data) + len(newline))
                                yield data, self._starting_offset, self._ending_offset, newline, None

                            if self._is_a_json_object_circuit_broken:
                                # let's yield what we have so far
                                for line, _, _, original_newline, _ in self._by_lines_fallback(self._unfinished_line):
                                    self._handle_offset(len(line) + len(original_newline))

                                    yield line, self._starting_offset, self._ending_offset, original_newline, None

                                # let's set the flag for direct yield from now on
                                has_an_object_start = False

                                # let's reset the buffer
                                self._unfinished_line = b""

            # in this case we could have a trailing new line in what's left in the buffer
            # or the content had a leading `{` but was not a json object before the circuit breaker intercepted it,
            # or we waited for the object start and never reached:
            # let's fallback to by_lines()
            if not self._is_a_json_object:
                buffer: bytes = self._unfinished_line
                if wait_for_object_start:
                    buffer = wait_for_object_start_buffer

                for line, _, _, newline, _ in self._by_lines_fallback(buffer):
                    line = line.strip(newline)
                    self._handle_offset(len(line) + len(newline))

                    yield line, self._starting_offset, self._ending_offset, newline, None


def inflate(func: GetByLinesCallable[ProtocolStorageType]) -> GetByLinesCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning inflated content in case the original is gzipped
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]:
        iterator = func(storage, range_start, body, is_gzipped)
        for data, _, _, _, _ in iterator:
            if is_gzipped:
                gzip_stream = gzip.GzipFile(fileobj=data)  # type:ignore
                gzip_stream.seek(range_start)
                while True:
                    inflated_chunk: bytes = gzip_stream.read(CHUNK_SIZE)
                    if len(inflated_chunk) == 0:
                        break

                    buffer: BytesIO = BytesIO()
                    buffer.write(inflated_chunk)

                    shared_logger.debug("inflate inflate")
                    yield buffer.getvalue(), 0, 0, b"", None
            else:
                shared_logger.debug("inflate plain")
                yield data, 0, 0, b"", None

    return wrapper
