# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import gzip
import random
from typing import Union
from unittest import TestCase

import pytest

from storage import PayloadStorage, StorageReader

from .test_benchmark import (
    _IS_JSON,
    _IS_JSON_LIKE,
    _IS_PLAIN,
    _LENGTH_ABOVE_THRESHOLD,
    _LENGTH_BELOW_THRESHOLD,
    MockContentBase,
)


class MockContent(MockContentBase):
    @staticmethod
    def init_content(content_type: str, newline: bytes, length_multiplier: int = _LENGTH_ABOVE_THRESHOLD) -> None:
        MockContentBase.init_content(content_type=content_type, newline=newline, length_multiplier=length_multiplier)

        MockContent.f_content_gzip = base64.b64encode(gzip.compress(MockContentBase.mock_content))
        MockContent.f_content_plain = base64.b64encode(MockContentBase.mock_content)

        MockContent.f_size_gzip = len(MockContent.f_content_gzip)
        MockContent.f_size_plain = len(MockContent.f_content_plain)


@pytest.mark.unit
class TestPayloadStorage(TestCase):
    def test_get_as_string(self) -> None:
        MockContent.init_content(content_type=_IS_PLAIN, newline=b"\n")

        with self.subTest("testing with plain"):
            original = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
            payload_storage = PayloadStorage(payload=original)
            content = payload_storage.get_as_string()
            assert content == original
            assert len(content) == len(original)

        with self.subTest("testing with base64"):
            payload_storage = PayloadStorage(payload=MockContent.f_content_plain.decode("utf-8"))
            content = payload_storage.get_as_string()
            original = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
            assert content == original
            assert len(content) == len(original)

        with self.subTest("testing with gzip"):
            payload_storage = PayloadStorage(payload=MockContent.f_content_gzip.decode("utf-8"))
            content = payload_storage.get_as_string()
            original = gzip.decompress(base64.b64decode(MockContent.f_content_gzip)).decode("utf-8")

            assert content == original
            assert len(content) == len(original)

    def test_get_by_lines(self) -> None:
        for length_multiplier in [_LENGTH_BELOW_THRESHOLD, _LENGTH_ABOVE_THRESHOLD]:
            for content_type in [_IS_PLAIN, _IS_JSON, _IS_JSON_LIKE]:
                for newline in [b"", b"\n", b"\r\n"]:
                    with self.subTest(
                        f"testing with newline length {len(newline)} for content type {content_type}",
                        newline=newline,
                    ):
                        MockContent.init_content(
                            content_type=content_type, newline=newline, length_multiplier=length_multiplier
                        )

                        payload_content_gzip = MockContent.f_content_gzip.decode("utf-8")
                        payload_content_plain = MockContent.f_content_plain.decode("utf-8")

                        joiner_token: bytes = newline
                        if content_type is _IS_JSON:
                            joiner_token += newline

                        original: bytes = base64.b64decode(MockContent.f_content_plain)
                        original_length: int = len(original)

                        if content_type is _IS_JSON and original.endswith(newline * 2):
                            original_length -= len(newline)

                        payload_storage = PayloadStorage(payload=payload_content_gzip)
                        gzip_full: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=0)
                        )

                        payload_storage = PayloadStorage(payload=payload_content_plain)
                        plain_full: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=0)
                        )

                        diff = set(gzip_full) ^ set(plain_full)
                        assert not diff
                        assert plain_full == gzip_full
                        assert gzip_full[-1][1] == original_length
                        assert plain_full[-1][1] == original_length

                        joined = joiner_token.join([x[0] for x in plain_full])  # type:ignore
                        if original.endswith(newline):
                            joined += newline

                        assert joined == original

                        if len(newline) == 0:
                            continue

                        gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
                        plain_full_01 = plain_full[: int(len(plain_full) / 2)]

                        range_start = plain_full_01[-1][1]
                        payload_storage = PayloadStorage(payload=payload_content_gzip)
                        gzip_full_02: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=range_start)
                        )

                        payload_storage = PayloadStorage(payload=payload_content_plain)
                        plain_full_02: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
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

                        joined = (
                            joiner_token.join([x[0] for x in plain_full_01])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_02])  # type:ignore
                        )
                        if original.endswith(newline):
                            joined += newline

                        assert joined == original

                        gzip_full_02 = gzip_full_02[: int(len(gzip_full_02) / 2)]
                        plain_full_02 = plain_full_02[: int(len(plain_full_02) / 2)]

                        range_start = plain_full_02[-1][1]
                        payload_storage = PayloadStorage(payload=payload_content_gzip)
                        gzip_full_03: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=range_start)
                        )

                        payload_storage = PayloadStorage(payload=payload_content_plain)
                        plain_full_03: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
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

                        joined = (
                            joiner_token.join([x[0] for x in plain_full_01])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_02])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_03])  # type:ignore
                        )
                        if original.endswith(newline):
                            joined += newline

                        assert joined == original

                        range_start = plain_full[-1][1] + random.randint(1, 100)

                        payload_storage = PayloadStorage(payload=payload_content_gzip)
                        gzip_full_empty: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=range_start)
                        )

                        payload_storage = PayloadStorage(payload=payload_content_plain)
                        plain_full_empty: list[tuple[Union[StorageReader, bytes], int, int, int]] = list(
                            payload_storage.get_by_lines(range_start=range_start)
                        )

                        assert not gzip_full_empty
                        assert not plain_full_empty
