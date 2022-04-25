# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
import io
import random
from typing import Any, Optional, Union
from unittest import TestCase

import mock
import pytest
from botocore.response import StreamingBody

from storage import S3Storage, StorageReader

from .test_benchmark import (
    _IS_JSON,
    _IS_JSON_LIKE,
    _IS_PLAIN,
    _LENGTH_ABOVE_THRESHOLD,
    _LENGTH_BELOW_THRESHOLD,
    MockContentBase,
)


class MockContent(MockContentBase):
    f_stream_gzip: Optional[io.BytesIO] = None
    f_stream_plain: Optional[io.BytesIO] = None

    @staticmethod
    def rewind() -> None:
        assert MockContent.f_stream_gzip is not None
        assert MockContent.f_stream_plain is not None
        MockContent.f_stream_gzip.seek(0)
        MockContent.f_stream_plain.seek(0)

    @staticmethod
    def init_content(content_type: str, newline: bytes, length_multiplier: int = _LENGTH_ABOVE_THRESHOLD) -> None:
        MockContentBase.init_content(content_type=content_type, newline=newline, length_multiplier=length_multiplier)

        MockContent.f_content_plain = MockContentBase.mock_content
        MockContent.f_content_gzip = gzip.compress(MockContent.f_content_plain)
        MockContent.f_stream_gzip = io.BytesIO(MockContent.f_content_gzip)
        MockContent.f_stream_plain = io.BytesIO(MockContent.f_content_plain)
        MockContent.rewind()

        MockContent.f_size_gzip = len(MockContent.f_content_gzip)
        MockContent.f_size_plain = len(MockContent.f_content_plain)

    @staticmethod
    def s3_client_head_object(Bucket: str, Key: str) -> dict[str, Any]:
        content_type = "text/plain"
        content_length = MockContent.f_size_plain
        if Key.endswith(".gz"):
            content_type = "application/x-gzip"
            content_length = MockContent.f_size_gzip

        return {"ContentType": content_type, "ContentLength": content_length}

    @staticmethod
    def s3_client_get_object(Bucket: str, Key: str, Range: str) -> dict[str, Any]:
        range_int: int = int(Range.replace("bytes=", "", -1).split("-")[0])
        assert MockContent.f_stream_plain is not None
        MockContent.f_stream_plain.seek(range_int)
        content_body = MockContent.f_stream_plain
        content_length = len(MockContent.f_content_plain[range_int:])
        if Key.endswith(".gz"):
            assert MockContent.f_stream_gzip is not None
            MockContent.f_stream_gzip.seek(range_int)
            content_body = MockContent.f_stream_gzip
            content_length = MockContent.f_size_gzip

        return {"Body": StreamingBody(content_body, content_length), "ContentLength": content_length}

    @staticmethod
    def s3_client_download_fileobj(Bucket: str, Key: str, Fileobj: io.BytesIO) -> None:
        if Key.endswith(".gz"):
            assert MockContent.f_stream_gzip is not None
            Fileobj.writelines(MockContent.f_stream_gzip.readlines())
        else:
            assert MockContent.f_stream_plain is not None
            Fileobj.writelines(MockContent.f_stream_plain.readlines())


@pytest.mark.unit
class TestS3Storage(TestCase):
    @mock.patch("storage.S3Storage._s3_client.get_object", new=MockContent.s3_client_get_object)
    def test_get_as_string(self) -> None:
        MockContent.init_content(content_type=_IS_PLAIN, newline=b"\n")

        s3_storage = S3Storage(bucket_name="dummy_bucket", object_key="dummy.key")
        content: bytes = s3_storage.get_as_string().encode("UTF-8")
        assert content == MockContent.f_content_plain
        assert len(content) == len(MockContent.f_content_plain)

    @mock.patch("storage.S3Storage._s3_client.head_object", new=MockContent.s3_client_head_object)
    @mock.patch("storage.S3Storage._s3_client.download_fileobj", new=MockContent.s3_client_download_fileobj)
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

                        joiner_token: bytes = newline

                        original_length: int = MockContent.f_size_plain

                        if content_type is _IS_JSON and MockContent.f_content_plain.endswith(newline * 2):
                            original_length -= len(newline)

                        bucket_name: str = "dummy_bucket"

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                        gzip_full: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=0))

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                        plain_full: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=0))

                        diff = set(gzip_full) ^ set(plain_full)
                        assert not diff
                        assert plain_full == gzip_full
                        assert gzip_full[-1][2] == original_length
                        assert plain_full[-1][2] == original_length

                        joined = joiner_token.join([x[0] for x in plain_full])  # type:ignore
                        if MockContent.f_content_plain.endswith(newline):
                            joined += newline

                        assert joined == MockContent.f_content_plain

                        if len(newline) == 0:
                            continue

                        gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
                        plain_full_01 = plain_full[: int(len(plain_full) / 2)]

                        MockContent.rewind()

                        range_start = plain_full_01[-1][2]
                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                        gzip_full_02: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                        plain_full_02: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

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
                            joiner_token.join([x[0] for x in plain_full_01])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_02])  # type:ignore
                        )
                        if MockContent.f_content_plain.endswith(newline):
                            joined += newline

                        assert joined == MockContent.f_content_plain

                        MockContent.rewind()

                        gzip_full_02 = gzip_full_02[: int(len(gzip_full_02) / 2)]
                        plain_full_02 = plain_full_02[: int(len(plain_full_02) / 2)]

                        range_start = plain_full_02[-1][2]
                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                        gzip_full_03: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                        plain_full_03: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

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
                            joiner_token.join([x[0] for x in plain_full_01])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_02])  # type:ignore
                            + joiner_token
                            + joiner_token.join([x[0] for x in plain_full_03])  # type:ignore
                        )
                        if MockContent.f_content_plain.endswith(newline):
                            joined += newline

                        assert joined == MockContent.f_content_plain

                        MockContent.rewind()

                        range_start = plain_full[-1][2] + random.randint(1, 100)

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                        gzip_full_empty: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

                        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                        plain_full_empty: list[
                            tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]
                        ] = list(s3_storage.get_by_lines(range_start=range_start))

                        assert not gzip_full_empty
                        assert not plain_full_empty
