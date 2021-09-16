from typing import Generator

import boto3
import elasticapm  # noqa: F401
from botocore.response import StreamingBody

from share import ByLines, Deflate, logger

from .storage import CommonStorage


class S3Storage(CommonStorage):
    _chunk_size: int = 1024 * 1024

    def __init__(self, bucket_name: str, object_key: str):
        self._bucket_name: str = bucket_name
        self._object_key: str = object_key

        # Get the service resource
        self._s3_client = boto3.client("s3")

    @ByLines
    @Deflate
    def _generate(
        self, range_start: int, last_decorator_offset: int, body: StreamingBody, content_type: str
    ) -> Generator[tuple[bytes, int, int], None, None]:
        previous_length: int = last_decorator_offset
        # `decorator_offset` starts from `range_start`
        decorator_offset: int = range_start
        for chunk in iter(lambda: body.read(self._chunk_size), b""):
            chunk_length: int = len(chunk)
            range_offset: int = range_start + chunk_length
            # `decorator_offset` should be the beginning of
            # the chunk position, not the length of it
            decorator_offset += previous_length

            # `previous_length` can now be updated in order for the
            # next iteration to be added to `decorator_offset`
            previous_length += chunk_length

            logger.debug("_generate", extra={"offset": decorator_offset})
            yield chunk, range_offset, decorator_offset

    def get_by_lines(
        self, range_start: int, last_decorator_offset: int
    ) -> Generator[tuple[bytes, int, int], None, None]:
        logger.debug("get_by_lines", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})
        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_name, Key=self._object_key, Range=f"bytes={range_start}-"
        )

        return self._generate(range_start, last_decorator_offset, s3_object["Body"], s3_object["ContentType"])

    def get_as_string(self) -> str:
        logger.debug("get_as_string", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})
        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_name,
            Key=self._object_key,
        )

        body: StreamingBody = s3_object["Body"]
        return body.read(s3_object["ContentLength"]).decode("UTF-8")
