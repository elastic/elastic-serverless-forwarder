import re
import zlib
from typing import Generator

_d = zlib.decompressobj(16 + zlib.MAX_WBITS)


def by_line(func):
    def wrapper(*args) -> Generator[tuple[bytes, int], None, None]:
        offset = 0
        unfinished_line: bytes = b''
        for data, length in func(*args):
            buffer = unfinished_line + data
            lines = buffer.decode('UTF-8').splitlines()
            if len(lines) > 0:
                unfinished_line = lines.pop().encode()

            for line in lines:
                yield line.encode(), offset
                offset += len(line)

        yield unfinished_line, offset

    return wrapper


def deflate(func):
    def wrapper(*args) -> Generator[tuple[bytes, int], None, None]:
        for data, length in func(*args):
            if args[2] == "application/x-gzip":
                decoded = _d.decompress(data)
                yield decoded, len(decoded)
            else:
                yield data, length

    return wrapper
