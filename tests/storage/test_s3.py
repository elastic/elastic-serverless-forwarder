import gzip
import io
import random
import string
from typing import Any, Optional
from unittest import TestCase

import mock
from botocore.response import StreamingBody

import storage
from storage import S3Storage


class MockContent:
    f_size_plain: int = 0
    f_size_gzip: int = 0
    f_content: bytes = b""
    f_stream_plain: Optional[io.BytesIO] = None
    f_stream_gzip: Optional[io.BytesIO] = None

    @staticmethod
    def rewind() -> None:
        assert MockContent.f_stream_gzip is not None
        assert MockContent.f_stream_plain is not None
        MockContent.f_stream_gzip.seek(0)
        MockContent.f_stream_plain.seek(0)

    @staticmethod
    def init_content() -> None:
        MockContent.f_content += "\n".join(
            [
                "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20)))
                for _ in range(0, storage.CHUNK_SIZE)
            ]
        ).encode("UTF-8")

        f_content_gzip: bytes = gzip.compress(MockContent.f_content)
        MockContent.f_stream_gzip = io.BytesIO(f_content_gzip)
        MockContent.f_stream_plain = io.BytesIO(MockContent.f_content)
        MockContent.f_stream_gzip.seek(0)
        MockContent.f_stream_plain.seek(0)

        MockContent.f_size_gzip = len(f_content_gzip)
        MockContent.f_size_plain = len(MockContent.f_content)

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
        content_length = len(MockContent.f_content[range_int:])
        if Key.endswith(".gz"):
            assert MockContent.f_stream_gzip is not None
            content_body = MockContent.f_stream_gzip
            content_length = MockContent.f_size_gzip

        return {"Body": StreamingBody(content_body, content_length), "ContentLength": content_length}


class TestS3Storage(TestCase):
    @mock.patch("storage.S3Storage._s3_client.get_object", new=MockContent.s3_client_get_object)
    def test_get_as_string(self) -> None:
        mock_content = MockContent()
        mock_content.init_content()

        s3_storage = S3Storage(bucket_name="dummy_bucket", object_key="dummy.key")
        content: bytes = s3_storage.get_as_string().encode("UTF-8")
        assert content == mock_content.f_content
        assert len(content) == len(mock_content.f_content)

    @mock.patch("storage.S3Storage._s3_client.head_object", new=MockContent.s3_client_head_object)
    @mock.patch("storage.S3Storage._s3_client.get_object", new=MockContent.s3_client_get_object)
    def test_get_by_lines(self) -> None:
        mock_content = MockContent()
        mock_content.init_content()

        bucket_name: str = "dummy_bucket"

        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
        gzip_full: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=0))

        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
        plain_full: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=0))

        diff = set(gzip_full) ^ set(plain_full)
        assert not diff
        assert plain_full == gzip_full
        assert plain_full[-1][1] == len(mock_content.f_content)
        assert "\n".join([x[0].decode("UTF-8") for x in plain_full]).encode("UTF-8") == mock_content.f_content

        mock_content.rewind()

        gzip_full_01 = gzip_full[: int(len(gzip_full) / 2)]
        plain_full_01 = plain_full[: int(len(plain_full) / 2)]

        mock_content.rewind()

        range_start = plain_full_01[-1][1]
        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
        gzip_full_02: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=range_start))

        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
        plain_full_02: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=range_start))

        diff = set(gzip_full_01) ^ set(plain_full_01)
        assert not diff
        assert plain_full_01 == gzip_full_01

        diff = set(gzip_full_02) ^ set(plain_full_02)
        assert not diff
        assert plain_full_02 == gzip_full_02
        assert plain_full_01[-1][1] + plain_full_02[-1][1] == len(mock_content.f_content)
        assert (
            "\n".join([x[0].decode("UTF-8") for x in plain_full_01]).encode("UTF-8")
            + b"\n"
            + "\n".join([x[0].decode("UTF-8") for x in plain_full_02]).encode("UTF-8")
            == mock_content.f_content
        )

        mock_content.rewind()

        range_start = plain_full[-1][1] + random.randint(1, 100)

        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key.gz")
        gzip_full_empty: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=range_start))

        s3_storage = S3Storage(bucket_name=bucket_name, object_key="dummy.key")
        plain_full_empty: list[tuple[bytes, int]] = list(s3_storage.get_by_lines(range_start=range_start))

        assert not gzip_full_empty
        assert not plain_full_empty
