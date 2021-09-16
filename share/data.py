import zlib
from functools import partial
from typing import Generator

from .logger import logger


class ByLines:
    def __init__(self, function):
        self._function = function

        self._offset: int = 0
        self._last_range_offset: int = 0
        self._last_decorator_offset: int = 0

    def __call__(self, instance, *args) -> Generator[tuple[bytes, int, int], None, None]:
        unfinished_line: bytes = b""
        self._last_decorator_offset: int = args[1]
        for data, range_offset, decorator_offset in self._function(instance, *args):
            # `self._offset` will contains this decorator offset and should be
            # the beginning of the line position, not the length of it
            # we assume that `decorator_offset` is correct and we don't
            # rely on `last_decorator_offset`
            self._offset: int = decorator_offset

            self._last_range_offset = range_offset

            unfinished_line = unfinished_line + data
            lines = unfinished_line.decode("UTF-8").splitlines()

            if len(lines) > 0:
                unfinished_line = lines.pop().encode()

            for line in lines:
                offset: int = self._offset
                logger.debug("by_line lines", extra={"offset": offset})
                # we increase `self._offset` with the latest length for the next iteration
                self._offset += len(line)

                yield line.encode(), range_offset, offset

        logger.debug("by_line unfinished_line", extra={"offset": self._offset})
        if len(unfinished_line) > 0:
            yield unfinished_line, self._last_range_offset, self._offset

    def __get__(self, instance, owner):
        return partial(self, instance)


class Deflate:
    def __init__(self, function):
        self._function = function
        self._previous_length: int = 0

    def __call__(self, instance, *args) -> Generator[tuple[bytes, int, int], None, None]:
        for data, range_offset, decorator_offset in self._function(instance, *args):
            last_decorator_offset = args[1]
            if args[3] == "application/x-gzip":
                _d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                decoded: bytes = _d.decompress(data)
                length_decode: int = len(decoded)
                # `offset` will contains this decorator offset and should be
                # the beginning of the chunk position, not the length of it.
                # since here the content is compressed we cannot rely on
                # `decorator_offset` but rather on `last_decorator_offset`
                offset: int = last_decorator_offset + self._previous_length
                self._previous_length += length_decode
                logger.debug("deflate decompress", extra={"offset": offset})
                yield decoded, range_offset, offset
            else:
                logger.debug("deflate plan", extra={"offset": decorator_offset})
                yield data, range_offset, decorator_offset

    def __get__(self, instance, owner):
        return partial(self, instance)
