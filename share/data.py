import zlib
from typing import Generator

from .logger import logger


def by_line(func):
    def wrapper(*args) -> Generator[tuple[bytes, int], None, None]:
        offset = 0
        unfinished_line: bytes = b""
        for data, length in func(*args):
            unfinished_line = unfinished_line + data
            lines = unfinished_line.decode("UTF-8").splitlines()
            if len(lines) > 0:
                unfinished_line = lines.pop().encode()

            for line in lines:
                logger.debug("by_line lines", extra={"offset": offset})
                yield line.encode(), offset
                offset += len(line)

        logger.debug("by_line unfinished_line", extra={"offset": offset})
        yield unfinished_line, offset

    return wrapper


def deflate(func):
    def wrapper(*args) -> Generator[tuple[bytes, int], None, None]:
        for data, length in func(*args):
            if args[2] == "application/x-gzip":
                _d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                decoded = _d.decompress(data)
                logger.debug("deflate decompress", extra={"offset": len(decoded)})
                yield decoded, len(decoded)
            else:
                logger.debug("deflate plan", extra={"offset": length})
                yield data, length

    return wrapper
