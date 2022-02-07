# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import gzip
import io
import random
import string
from typing import Optional, Union
from unittest import TestCase

import pytest

from storage import PayloadStorage, StorageReader

_1M: int = 1024**2


class MockContent:
    f_size_gzip: int = 0
    f_size_plain: int = 0
    f_content_gzip: str = ""
    f_content_plain: str = ""
    f_stream_gzip: Optional[io.BytesIO] = None
    f_stream_plain: Optional[io.BytesIO] = None

    @staticmethod
    def rewind() -> None:
        assert MockContent.f_stream_gzip is not None
        assert MockContent.f_stream_plain is not None
        MockContent.f_stream_gzip.seek(0)
        MockContent.f_stream_plain.seek(0)

    @staticmethod
    def init_content(newline: str) -> None:
        if len(newline) == 0:
            mock_content = "".join(
                random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20))
            ).encode("UTF-8")
        else:
            # every line is from 0 to 20 chars, repeated for 1M: a few megabytes of content
            mock_content = newline.join(
                [
                    "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20)))
                    for _ in range(0, _1M)
                ]
            ).encode("UTF-8")

        MockContent.f_content_gzip = base64.b64encode(gzip.compress(mock_content)).decode("utf-8")
        MockContent.f_content_plain = base64.b64encode(mock_content).decode("utf-8")
        MockContent.f_stream_gzip = io.StringIO(MockContent.f_content_gzip)
        MockContent.f_stream_plain = io.StringIO(MockContent.f_content_plain)
        MockContent.rewind()

        MockContent.f_size_gzip = len(MockContent.f_content_gzip)
        MockContent.f_size_plain = len(MockContent.f_content_plain)

@pytest.mark.unit
class TestPayloadStorage(TestCase):
    def test_get_as_string(self) -> None:
        newline: str = "\n"
        MockContent.init_content(newline)

        payload_storage = PayloadStorage(payload=MockContent.f_content_plain)
        content: str = payload_storage.get_as_string()
        original: str = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
        assert content == original
        assert len(content) == len(original)

    def test_get_by_lines(self) -> None:
        for newline in ["", "\n", "\r\n"]:
            with self.subTest(f"testing with newline length {len(newline)}", newline=newline):
                MockContent.init_content(newline)

                original: str = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
                original_length: int = len(original) + len(newline)

                payload_storage = PayloadStorage(payload=MockContent.f_content_gzip)
                gzip_full: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=0)
                )

                payload_storage = PayloadStorage(payload=MockContent.f_content_plain)
                plain_full: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=0)
                )

                diff = set(gzip_full) ^ set(plain_full)
                assert not diff
                assert plain_full == gzip_full
                assert gzip_full[-1][1] == original_length
                assert plain_full[-1][1] == original_length
                assert (
                    newline.join([x[0].decode("UTF-8") for x in plain_full])
                    == original
                )

                if len(newline) == 0:
                    continue

                gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
                plain_full_01 = plain_full[: int(len(plain_full) / 2)]

                MockContent.rewind()

                range_start = plain_full_01[-1][1]
                payload_storage = PayloadStorage(payload=MockContent.f_content_gzip)
                gzip_full_02: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                payload_storage = PayloadStorage(payload=MockContent.f_content_plain)
                plain_full_02: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                diff = set(gzip_full_01) ^ set(plain_full_01)
                assert not diff
                assert plain_full_01 == gzip_full_01

                diff = set(gzip_full_02) ^ set(plain_full_02)
                assert not diff
                assert plain_full_02 == gzip_full_02

                assert plain_full_01 + plain_full_02 == plain_full
                assert gzip_full_02[-1][1] == original_length
                assert plain_full_02[-1][1] == original_length
                assert (
                    newline.join([x[0].decode("UTF-8") for x in plain_full_01])
                    + newline
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_02])
                    == original
                )

                MockContent.rewind()

                gzip_full_02 = gzip_full_02[: int(len(gzip_full_02) / 2)]
                plain_full_02 = plain_full_02[: int(len(plain_full_02) / 2)]

                range_start = plain_full_02[-1][1]
                payload_storage = PayloadStorage(payload=MockContent.f_content_gzip)
                gzip_full_03: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                payload_storage = PayloadStorage(payload=MockContent.f_content_plain)
                plain_full_03: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                diff = set(gzip_full_02) ^ set(plain_full_02)
                assert not diff
                assert plain_full_02 == gzip_full_02

                diff = set(gzip_full_03) ^ set(plain_full_03)
                assert not diff
                assert plain_full_03 == gzip_full_03

                assert plain_full_01 + plain_full_02 + plain_full_03 == plain_full
                assert gzip_full_03[-1][1] == original_length
                assert plain_full_03[-1][1] == original_length
                assert (
                    newline.join([x[0].decode("UTF-8") for x in plain_full_01])
                    + newline
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_02])
                    + newline
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_03])
                    == original
                )

                MockContent.rewind()

                range_start = plain_full[-1][1] + random.randint(1, 100)

                payload_storage = PayloadStorage(payload=MockContent.f_content_gzip)
                gzip_full_empty: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                payload_storage = PayloadStorage(payload=MockContent.f_content_plain)
                plain_full_empty: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    payload_storage.get_by_lines(range_start=range_start)
                )

                assert not gzip_full_empty
                assert not plain_full_empty
