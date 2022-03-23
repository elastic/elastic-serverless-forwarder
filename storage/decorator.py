# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
import json
from functools import partial
from io import BytesIO
from typing import Any, Iterator, Union

from share import shared_logger

from .storage import CHUNK_SIZE, CommonStorageType, GetByLinesCallable, StorageReader


class JsonCollector:
    """
    CommonStorageType decorator for returning content by collected json object (if any) spanning multiple lines
    """

    def __init__(self, function: GetByLinesCallable[CommonStorageType]):
        self._function: GetByLinesCallable[CommonStorageType] = function

        self._json_decoder: json.JSONDecoder = json.JSONDecoder()

        self._offset: int = 0
        self._unfinished_line: bytes = b""

    def __call__(
        self, storage: CommonStorageType, range_start: int, body: BytesIO, content_type: str, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:
        is_first_iteration: bool = True
        has_an_object_start: bool = False

        self._offset = range_start

        iterator = self._function(storage, range_start, body, content_type, content_length)
        for data, original_line_ending_offset, newline_length in iterator:
            assert isinstance(data, bytes)

            self._newline_length = newline_length

            # let's check the first entry from content split by lines
            if is_first_iteration:
                # check only once
                is_first_iteration = False
                # we check for a potential json object on the first line
                # we strip leading space to be safe on padding
                stripped_data = data.decode("utf-8").lstrip()
                if len(stripped_data) > 0 and stripped_data[0] == "{":
                    # we mark the potentiality of a json object start
                    # CAVEAT: if the first log entry start with `{` but
                    # the content is not json we buffer all the content
                    # @TODO: not content will be yield as well, we must handle this scenario!
                    has_an_object_start = True

            # if it's not a json object we can just forward the content by lines
            if not has_an_object_start:
                yield data, original_line_ending_offset, newline_length
            else:
                for data_to_yield in self._collector(data):
                    shared_logger.debug("JsonCollector objects", extra={"offset": self._offset})
                    yield data_to_yield, self._offset, self._newline_length

        if has_an_object_start:
            # trailing content after last iteration
            for data_to_yield in self._collector():
                shared_logger.debug("JsonCollector last object", extra={"offset": self._offset})
                yield data_to_yield, self._offset, self._newline_length

    def _collector(self, data: bytes = b"") -> Iterator[bytes]:
        try:
            # let's buffer the content
            self._unfinished_line += data

            # we always receive newline stripped content, let's append
            # newline to the buffer
            if self._newline_length == 2:
                self._unfinished_line += b"\r\n"
            elif self._newline_length == 1:
                self._unfinished_line += b"\n"

            # let's remove leading newline in the buffer
            if self._unfinished_line.startswith(b"\r\n"):
                self._unfinished_line = self._unfinished_line.lstrip(b"\r\n")
            elif self._unfinished_line.startswith(b"\n"):
                self._unfinished_line = self._unfinished_line.lstrip(b"\n")

            # let's try to raw_decode (returning offset of trailing content after the end of the object)
            _, object_ending_offset = self._json_decoder.raw_decode(self._unfinished_line.decode("utf-8"))

            # it didn't raise: we collected a json object
            # the data to yield is from the last ending offset (starting as 0)
            # till the offset of the trailing content
            data_to_yield = self._unfinished_line[:object_ending_offset]

            # we cut the buffer from the length of the collected json object
            self._unfinished_line = self._unfinished_line[object_ending_offset:]

            # let's increase the last ending offset for yielding
            self._offset += object_ending_offset + self._newline_length

            # finally yield
            yield data_to_yield
        # it raised: we didn't collect enough content to reach the end of the json object: let's keep iterating
        except json.JSONDecodeError:
            pass

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

            if len(lines) == 0:
                continue

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
