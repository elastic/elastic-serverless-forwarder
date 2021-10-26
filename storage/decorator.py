# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from io import BytesIO
from typing import Any, Iterator, Union

from share import shared_logger

from .storage import CHUNK_SIZE, CommonStorageType, GetByLinesCallable, StorageReader


def by_lines(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
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
                if unfinished_line.find(b"\r\n") > 0:
                    newline = b"\r\n"
                    newline_length = len(newline)
                elif unfinished_line.find(b"\n") > 0:
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
            shared_logger.debug("by_line unfinished_line", extra={"offset": offset})

            yield unfinished_line, offset, newline_length

    return wrapper


def inflate(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
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
