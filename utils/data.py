import re
import zlib
from typing import Generator

_d = zlib.decompressobj(16 + zlib.MAX_WBITS)


def by_line(func):
    def wrapper(*args) -> Generator[(bytes, int), None, None]:
        offset = 0
        unfinished_line = ''
        for data, length in func(*args):
            data = unfinished_line + data
            lines = re.split(r'[\n\r]+', data)
            unfinished_line = lines.pop()
            for line in lines:
                yield line, offset
                offset += len(line)

        yield unfinished_line, offset

    return wrapper


def deflate(func):
    def wrapper(*args) -> Generator[(bytes, int), None, None]:
        for data, length in func(*args):
            if args[1] == "application/x-gzip":
                decoded = _d.decompress(data)
                yield decoded, len(decoded)
            else:
                yield data, length

    return wrapper
