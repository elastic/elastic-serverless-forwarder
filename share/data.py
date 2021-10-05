# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import zlib
from functools import partial
from typing import Generator

from .logger import logger


class ByLines:
    def __init__(self, function):
        self._function = function

        self._offset: int = 0
        self._last_ending_offset: int = 0
        self._last_beginning_offset: int = 0

    def __call__(self, instance, *args) -> Generator[tuple[bytes, int, int], None, None]:
        unfinished_line: bytes = b""
        self._last_beginning_offset: int = args[1]
        for data, beginning_offset, ending_offset in self._function(instance, *args):
            # `self._offset` will contains this decorator offset and should be
            # the beginning of the line position, not the length of it
            # we assume that `beginning_offset` is correct and we don't
            # rely on `last_beginning_offset`
            self._offset: int = beginning_offset

            self._last_ending_offset = ending_offset

            unfinished_line = unfinished_line + data
            lines = unfinished_line.decode("UTF-8").splitlines()

            if len(lines) > 0:
                unfinished_line = lines.pop().encode()

            for line in lines:
                offset: int = self._offset
                logger.debug("by_line lines", extra={"offset": offset})
                # we increase `self._offset` with the latest length for the next iteration
                self._offset += len(line)

                yield line.encode(), offset, ending_offset

        logger.debug("by_line unfinished_line", extra={"offset": self._offset})
        if len(unfinished_line) > 0:
            yield unfinished_line, self._offset, self._last_ending_offset

    def __get__(self, instance, owner):
        return partial(self, instance)


class Deflate:
    def __init__(self, function):
        self._function = function

    def __call__(self, instance, *args) -> Generator[tuple[bytes, int, int], None, None]:
        for data, beginning_offset, ending_offset in self._function(instance, *args):
            if args[3] == "application/x-gzip":
                last_beginning_offset: int = args[1]
                d = zlib.decompressobj(wbits=zlib.MAX_WBITS + 16)
                decoded: bytes = d.decompress(data)
                chunk = decoded[last_beginning_offset:]
                yield chunk, beginning_offset, ending_offset
            else:
                logger.debug("deflate plan", extra={"offset": beginning_offset})
                yield data, beginning_offset, ending_offset

    def __get__(self, instance, owner):
        return partial(self, instance)
