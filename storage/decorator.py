# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import zlib
from functools import partial
from typing import Any, Callable, Iterator

from share import shared_logger

from .storage import CommonStorageType


class ByLines:
    def __init__(self, function: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int]]]):
        self._new_line_length: int = len("\n")
        self._function: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int]]] = function

    def __call__(self, instance: CommonStorageType, *args: Any) -> Iterator[tuple[bytes, int]]:
        unfinished_line: bytes = b""
        self._offset: int = 0
        self._range_start: int = 0

        iterator = self._function(instance, *args)
        while True:
            try:
                data, range_start = next(iterator)
                if self._offset == 0:
                    self._offset = self._range_start

                unfinished_line = unfinished_line + data
                lines = unfinished_line.decode("UTF-8").splitlines()

                if len(lines) > 0:
                    if unfinished_line.endswith(b"\n"):
                        unfinished_line = lines.pop().encode() + b"\n"
                    else:
                        unfinished_line = lines.pop().encode()

                for line in lines:
                    line_encoded = line.encode("utf-8")
                    self._offset = self._offset + len(line_encoded) + self._new_line_length
                    shared_logger.debug("by_line lines", extra={"offset": self._offset})

                    yield line_encoded, self._offset

            except StopIteration:
                if len(unfinished_line) > 0:
                    self._offset = self._offset + len(unfinished_line)
                    shared_logger.debug("by_line unfinished_line", extra={"offset": self._offset})

                    yield unfinished_line, self._offset

                break

    def __get__(self, instance: CommonStorageType, owner: Any) -> partial[Iterator[tuple[bytes, int]]]:
        return partial(self, instance)


class Inflate:
    def __init__(self, function: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int]]]):
        self._function: Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int]]] = function

    def __call__(self, instance: CommonStorageType, *args: Any) -> Iterator[tuple[bytes, int]]:
        for data, range_start in self._function(instance, *args):
            if args[2] == "application/x-gzip":
                d = zlib.decompressobj(wbits=zlib.MAX_WBITS + 16)
                decoded: bytes = d.decompress(data)
                content_length: int = len(decoded)
                if range_start < content_length:
                    chunk = decoded[range_start:]
                    shared_logger.debug("inflate gzip", extra={"offset": range_start})
                    yield chunk, range_start
                else:
                    shared_logger.info(f"requested file content from {range_start}, file size {len(decoded)}: skip it")

            else:
                shared_logger.debug("inflate plain", extra={"offset": range_start})
                yield data, range_start

    def __get__(self, instance: CommonStorageType, owner: Any) -> partial[Iterator[tuple[bytes, int]]]:
        return partial(self, instance)
