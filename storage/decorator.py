# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import zlib
from typing import Any, Callable, Iterator

from share import shared_logger

from .storage import CommonStorageType


def by_lines(
    func: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int, int]]]
) -> Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int, int]]]:
    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[bytes, int, int]]:
        unfinished_line: bytes = b""
        _offset: int = 0
        _range_start: int = 0

        _newline: bytes = b""
        _newline_length: int = 0
        iterator = func(storage, range_start, body, content_type, content_length)
        while True:
            try:
                data, range_start, newline_length = next(iterator)
                if _offset == 0:
                    _offset = _range_start

                unfinished_line = unfinished_line + data
                lines = unfinished_line.decode("UTF-8").splitlines()

                if len(lines) > 0:
                    if _newline_length == 0:
                        if unfinished_line.find(b"\r\n") > 0:
                            _newline = b"\r\n"
                            _newline_length = len(_newline)
                        else:
                            _newline = b"\n"
                            _newline_length = len(_newline)

                    if unfinished_line.endswith(_newline):
                        unfinished_line = lines.pop().encode() + _newline
                    else:
                        unfinished_line = lines.pop().encode()

                for line in lines:
                    line_encoded = line.encode("UTF-8")
                    _offset = _offset + len(line_encoded) + _newline_length
                    shared_logger.debug("by_line lines", extra={"offset": _offset})

                    yield line_encoded, _offset, _newline_length

            except StopIteration:
                if len(unfinished_line) > 0:
                    _offset = _offset + len(unfinished_line)
                    shared_logger.debug("by_line unfinished_line", extra={"offset": _offset})

                    yield unfinished_line, _offset, _newline_length

                break

    return wrapper


def inflate(
    func: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int, int]]]
) -> Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int, int]]]:
    def wrapper(
        storage: CommonStorageType, range_start: int, body: Any, content_type: str, content_length: int
    ) -> Iterator[tuple[bytes, int, int]]:
        for data, range_start, newline_length in func(storage, range_start, body, content_type, content_length):
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
