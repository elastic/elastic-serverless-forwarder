# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
import io
import random
import string
from typing import Any, Optional, Union
from unittest import TestCase

import mock
import pytest
from botocore.response import StreamingBody

from storage import S3Storage, StorageReader

_1M: int = 1024**2


class MockContent:
    f_size_gzip: int = 0
    f_size_plain: int = 0
    f_content_gzip: bytes = b""
    f_content_plain: bytes = b""
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
            MockContent.f_content_plain = "".join(
                random.choices(string.ascii_letters + string.digits, k=random.randint(1, 20))
            ).encode("UTF-8")
        else:
            # every line is from 0 to 20 chars, repeated for 1M: a few megabytes of content
            MockContent.f_content_plain = newline.join(
                [
                    "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20)))
                    for _ in range(1, _1M)
                ]
            ).encode("UTF-8")

            if MockContent.f_content_plain.endswith(newline.encode("utf-8")):
                MockContent.f_content_plain = MockContent.f_content_plain[: 0 - len(newline.encode("utf-8"))]

            if len(MockContent.f_content_plain) == 0:
                MockContent.f_content_plain = "".join(
                    random.choices(string.ascii_letters + string.digits, k=random.randint(1, 20))
                ).encode("UTF-8")

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
        newline: str = "\n"
        MockContent.init_content(newline)

        s3_storage = S3Storage(bucket_name="dummy_bucket", object_key="dummy.key")
        content: bytes = s3_storage.get_as_string().encode("UTF-8")
        assert content == MockContent.f_content_plain
        assert len(content) == len(MockContent.f_content_plain)

    @mock.patch("storage.S3Storage._s3_client.head_object", new=MockContent.s3_client_head_object)
    @mock.patch("storage.S3Storage._s3_client.download_fileobj", new=MockContent.s3_client_download_fileobj)
    def test_get_by_lines(self) -> None:
        for newline in ["", "\n", "\r\n"]:
            with self.subTest(f"testing with newline length {len(newline)}", newline=newline):
                MockContent.init_content(newline)

                bucket_name: str = "dummy_bucket"

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                gzip_full: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=0)
                )

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                plain_full: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=0)
                )

                diff = set(gzip_full) ^ set(plain_full)
                assert not diff
                assert plain_full == gzip_full
                assert gzip_full[-1][1] == MockContent.f_size_plain
                assert plain_full[-1][1] == MockContent.f_size_plain

                joined = newline.join([x[0].decode("UTF-8") for x in plain_full]).encode("UTF-8")
                if MockContent.f_content_plain.endswith(newline.encode("UTF-8")):
                    joined += newline.encode("UTF-8")

                assert joined == MockContent.f_content_plain

                if len(newline) == 0:
                    continue

                gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
                plain_full_01 = plain_full[: int(len(plain_full) / 2)]

                MockContent.rewind()

                range_start = plain_full_01[-1][1]
                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                gzip_full_02: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                plain_full_02: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                diff = set(gzip_full_01) ^ set(plain_full_01)
                assert not diff
                assert plain_full_01 == gzip_full_01

                diff = set(gzip_full_02) ^ set(plain_full_02)
                assert not diff
                assert plain_full_02 == gzip_full_02

                assert plain_full_01 + plain_full_02 == plain_full
                assert gzip_full_02[-1][1] == MockContent.f_size_plain
                assert plain_full_02[-1][1] == MockContent.f_size_plain

                joined = (
                    newline.join([x[0].decode("UTF-8") for x in plain_full_01]).encode("UTF-8")
                    + newline.encode("UTF-8")
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_02]).encode("UTF-8")
                )
                if MockContent.f_content_plain.endswith(newline.encode("UTF-8")):
                    joined += newline.encode("UTF-8")

                assert joined == MockContent.f_content_plain

                MockContent.rewind()

                gzip_full_02 = gzip_full_02[: int(len(gzip_full_02) / 2)]
                plain_full_02 = plain_full_02[: int(len(plain_full_02) / 2)]

                range_start = plain_full_02[-1][1]
                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                gzip_full_03: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                plain_full_03: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                diff = set(gzip_full_02) ^ set(plain_full_02)
                assert not diff
                assert plain_full_02 == gzip_full_02

                diff = set(gzip_full_03) ^ set(plain_full_03)
                assert not diff
                assert plain_full_03 == gzip_full_03

                assert plain_full_01 + plain_full_02 + plain_full_03 == plain_full
                assert gzip_full_03[-1][1] == MockContent.f_size_plain
                assert plain_full_03[-1][1] == MockContent.f_size_plain

                joined = (
                    newline.join([x[0].decode("UTF-8") for x in plain_full_01]).encode("UTF-8")
                    + newline.encode("UTF-8")
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_02]).encode("UTF-8")
                    + newline.encode("UTF-8")
                    + newline.join([x[0].decode("UTF-8") for x in plain_full_03]).encode("UTF-8")
                )
                if MockContent.f_content_plain.endswith(newline.encode("UTF-8")):
                    joined += newline.encode("UTF-8")

                assert joined == MockContent.f_content_plain

                MockContent.rewind()

                range_start = plain_full[-1][1] + random.randint(1, 100)

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
                gzip_full_empty: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
                plain_full_empty: list[tuple[Union[StorageReader, bytes], int, int]] = list(
                    s3_storage.get_by_lines(range_start=range_start)
                )

                assert not gzip_full_empty
                assert not plain_full_empty
