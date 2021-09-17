from typing import Generator

import boto3
import elasticapm  # noqa: F401
from botocore.response import StreamingBody

from share import ByLines, Deflate, logger

from .storage import CommonStorage


class S3Storage(CommonStorage):
    _chunk_size: int = 1024 ** 2

    def __init__(self, bucket_name: str, object_key: str):
        self._bucket_name: str = bucket_name
        self._object_key: str = object_key

        # Get the service resource
        self._s3_client = boto3.client("s3")

    @ByLines
    @Deflate
    def _generate(
        self, range_start: int, last_beginning_offset: int, body: StreamingBody, content_type: str, content_length: int
    ) -> Generator[tuple[bytes, int, int], None, None]:
        if content_type == "application/x-gzip":
            chunk = body.read(content_length)
            logger.debug("_generate gzip", extra={"offset": 0})
            yield chunk, range_start, last_beginning_offset
        else:
            previous_length: int = last_beginning_offset
            # `beginning_offset` starts from `range_start`
            beginning_offset: int = range_start
            ending_offset: int = 0
            for chunk in iter(lambda: body.read(self._chunk_size), b""):
                chunk_length: int = len(chunk)
                ending_offset += range_start + chunk_length
                # `beginning_offset` should be the beginning of
                # the chunk position, not the length of it
                beginning_offset += previous_length

                # `previous_length` can now be updated in order for the
                # next iteration to be added to `beginning_offset`
                previous_length += chunk_length

                logger.debug("_generate flat", extra={"offset": beginning_offset})
                yield chunk, beginning_offset, ending_offset

    def get_by_lines(
        self, range_start: int, last_beginning_offset: int
    ) -> Generator[tuple[bytes, int, int], None, None]:
        logger.debug("get_by_lines", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})

        original_range_start: int = range_start
        s3_object_header = self._s3_client.get_object(Bucket=self._bucket_name, Key=self._object_key, Range="bytes=0-4")

        content_type: str = s3_object_header["ContentType"]
        if content_type == "application/x-gzip":
            range_start = 0

        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_name, Key=self._object_key, Range=f"bytes={range_start}-"
        )

        return self._generate(
            original_range_start,
            last_beginning_offset,
            s3_object["Body"],
            s3_object["ContentType"],
            s3_object["ContentLength"],
        )

    def get_as_string(self) -> str:
        logger.debug("get_as_string", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})
        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_name,
            Key=self._object_key,
        )

        body: StreamingBody = s3_object["Body"]
        return body.read(s3_object["ContentLength"]).decode("UTF-8")
