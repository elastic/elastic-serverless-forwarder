# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import datetime
import gzip
import random
from typing import Optional

import mock
import pytest

from storage import PayloadStorage

from .test_benchmark import (
    _IS_PLAIN,
    _LENGTH_ABOVE_THRESHOLD,
    MockContentBase,
    get_by_lines_parameters,
    multiline_processor,
)


class MockContent(MockContentBase):
    @staticmethod
    def init_content(
        content_type: str,
        newline: bytes,
        length_multiplier: int = _LENGTH_ABOVE_THRESHOLD,
        json_content_type: Optional[str] = None,
    ) -> None:
        MockContentBase.init_content(
            content_type=content_type,
            newline=newline,
            length_multiplier=length_multiplier,
            json_content_type=json_content_type,
        )

        MockContent.f_content_gzip = base64.b64encode(gzip.compress(MockContentBase.mock_content))
        MockContent.f_content_plain = base64.b64encode(MockContentBase.mock_content)
        MockContent.f_size_gzip = len(MockContent.f_content_gzip)
        MockContent.f_size_plain = len(MockContent.f_content_plain)


@pytest.mark.unit
def test_get_as_string_plain() -> None:
    MockContent.init_content(content_type=_IS_PLAIN, newline=b"\n")
    original = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
    payload_storage = PayloadStorage(payload=original)
    content = payload_storage.get_as_string()
    assert content == original
    assert len(content) == len(original)


@pytest.mark.unit
def test_get_as_string_base64() -> None:
    MockContent.init_content(content_type=_IS_PLAIN, newline=b"\n")
    payload_storage = PayloadStorage(payload=MockContent.f_content_plain.decode("utf-8"))
    content = payload_storage.get_as_string()
    original = base64.b64decode(MockContent.f_content_plain).decode("utf-8")
    assert content == original
    assert len(content) == len(original)


@pytest.mark.unit
def test_get_as_string_gzip() -> None:
    MockContent.init_content(content_type=_IS_PLAIN, newline=b"\n")
    payload_storage = PayloadStorage(payload=MockContent.f_content_gzip.decode("utf-8"))
    content = payload_storage.get_as_string()
    original = gzip.decompress(base64.b64decode(MockContent.f_content_gzip)).decode("utf-8")

    assert content == original
    assert len(content) == len(original)


@pytest.mark.unit
@pytest.mark.parametrize("length_multiplier,content_type,newline,json_content_type", get_by_lines_parameters())
@mock.patch("share.multiline.timedelta_circuit_breaker", new=datetime.timedelta(days=1))
def test_get_by_lines(
    length_multiplier: int, content_type: str, newline: bytes, json_content_type: Optional[str]
) -> None:
    MockContent.init_content(
        content_type=content_type,
        newline=newline,
        length_multiplier=length_multiplier,
        json_content_type=json_content_type,
    )

    payload_content_gzip = MockContent.f_content_gzip.decode("utf-8")
    payload_content_plain = MockContent.f_content_plain.decode("utf-8")

    joiner_token: bytes = newline

    original: bytes = base64.b64decode(MockContent.f_content_plain)
    original_length: int = len(original)

    payload_storage = PayloadStorage(
        payload=payload_content_gzip,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    gzip_full: list[tuple[bytes, int, int, Optional[int]]] = list(payload_storage.get_by_lines(range_start=0))

    payload_storage = PayloadStorage(
        payload=payload_content_plain,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    plain_full: list[tuple[bytes, int, int, Optional[int]]] = list(payload_storage.get_by_lines(range_start=0))

    diff = set(gzip_full) ^ set(plain_full)
    assert not diff
    assert plain_full == gzip_full
    assert gzip_full[-1][2] == original_length
    assert plain_full[-1][2] == original_length

    joined = joiner_token.join([x[0] for x in plain_full])
    assert joined == original

    if len(newline) == 0 or (json_content_type == "single"):
        return

    gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
    plain_full_01 = plain_full[: int(len(plain_full) / 2)]

    range_start = plain_full_01[-1][2]

    payload_storage = PayloadStorage(
        payload=payload_content_gzip,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    gzip_full_02: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    payload_storage = PayloadStorage(
        payload=payload_content_plain,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    plain_full_02: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    diff = set(gzip_full_01) ^ set(plain_full_01)
    assert not diff
    assert plain_full_01 == gzip_full_01

    diff = set(gzip_full_02) ^ set(plain_full_02)
    assert not diff
    assert plain_full_02 == gzip_full_02

    assert plain_full_01 + plain_full_02 == plain_full
    assert gzip_full_02[-1][2] == original_length
    assert plain_full_02[-1][2] == original_length

    joined = (
        joiner_token.join([x[0] for x in plain_full_01])
        + joiner_token
        + joiner_token.join([x[0] for x in plain_full_02])
    )

    assert joined == original

    gzip_full_02 = gzip_full_02[: int(len(gzip_full_02) / 2)]
    plain_full_02 = plain_full_02[: int(len(plain_full_02) / 2)]

    range_start = plain_full_02[-1][2]

    payload_storage = PayloadStorage(
        payload=payload_content_gzip,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    gzip_full_03: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    payload_storage = PayloadStorage(
        payload=payload_content_plain,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    plain_full_03: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    diff = set(gzip_full_02) ^ set(plain_full_02)
    assert not diff
    assert plain_full_02 == gzip_full_02

    diff = set(gzip_full_03) ^ set(plain_full_03)
    assert not diff
    assert plain_full_03 == gzip_full_03

    assert plain_full_01 + plain_full_02 + plain_full_03 == plain_full
    assert gzip_full_03[-1][2] == original_length
    assert plain_full_03[-1][2] == original_length

    joined = (
        joiner_token.join([x[0] for x in plain_full_01])
        + joiner_token
        + joiner_token.join([x[0] for x in plain_full_02])
        + joiner_token
        + joiner_token.join([x[0] for x in plain_full_03])
    )

    assert joined == original

    range_start = plain_full[-1][2] + random.randint(1, 100)

    payload_storage = PayloadStorage(
        payload=payload_content_gzip,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    gzip_full_empty: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    payload_storage = PayloadStorage(
        payload=payload_content_plain,
        json_content_type=json_content_type,
        multiline_processor=multiline_processor(content_type),
    )
    plain_full_empty: list[tuple[bytes, int, int, Optional[int]]] = list(
        payload_storage.get_by_lines(range_start=range_start)
    )

    assert not gzip_full_empty
    assert not plain_full_empty
