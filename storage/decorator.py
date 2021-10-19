# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import zlib
from typing import Any, Iterator

from share import shared_logger

from .storage import CommonStorageType, GetByLinesCallable


def by_lines(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[bytes, int, int]]:
        offset: int = range_start
        unfinished_line: bytes = b""

        newline: bytes = b""
        newline_length: int = 0
        iterator = func(storage, range_start, body, content_type, content_length)
        for data, range_start, _newline_length in iterator:
            unfinished_line += data
            lines = unfinished_line.decode("UTF-8").splitlines()

            if len(lines) == 0:
                continue

            if newline_length == 0:
                if unfinished_line.find(b"\r\n") > 0:
                    newline = b"\r\n"
                    newline_length = len(newline)
                else:
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
            shared_logger.debug("by_line unfinished_line", extra={"offset": offset})

            yield unfinished_line, offset, newline_length

    return wrapper


def inflate(func: GetByLinesCallable[CommonStorageType]) -> GetByLinesCallable[CommonStorageType]:
    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[bytes, int, int]]:
        iterator = func(storage, range_start, body, content_type, content_length)
        for data, range_start, newline_length in iterator:
            if content_type == "application/x-gzip":
                d = zlib.decompressobj(wbits=zlib.MAX_WBITS + 16)
                decoded: bytes = d.decompress(data)
                content_length = len(decoded)
                if range_start < content_length:
                    chunk = decoded[range_start:]
                    shared_logger.debug("inflate gzip", extra={"offset": range_start})
                    yield chunk, range_start, 0
                else:
                    shared_logger.info(f"requested file content from {range_start}, file size {len(decoded)}: skip it")

            else:
                shared_logger.debug("inflate plain", extra={"offset": range_start})
                yield data, range_start, 0

    return wrapper
