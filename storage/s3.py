# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Iterator

import boto3
import elasticapm  # noqa: F401
from botocore.response import StreamingBody

from share import shared_logger

from .decorator import by_lines, inflate
from .storage import CommonStorage

CHUNK_SIZE: int = 1024 ** 2


class S3Storage(CommonStorage):
    _s3_client = boto3.client("s3")

    def __init__(self, bucket_name: str, object_key: str):
        self._bucket_name: str = bucket_name
        self._object_key: str = object_key

    @by_lines
    @inflate
    def _generate(
        self, range_start: int, body: StreamingBody, content_type: str, content_length: int
    ) -> Iterator[tuple[bytes, int, int]]:
        if content_type == "application/x-gzip":
            chunk = body.read(content_length)
            shared_logger.debug("_generate gzip", extra={"offset": range_start})
            yield chunk, range_start, 0
        else:
            previous_length: int = range_start

            def chunk_lambda() -> Any:
                return body.read(CHUNK_SIZE)

            for chunk in iter(chunk_lambda, b""):
                previous_length += len(chunk)

                shared_logger.debug("_generate flat", extra={"offset": previous_length})
                yield chunk, previous_length, 0

    def get_by_lines(self, range_start: int) -> Iterator[tuple[bytes, int, int]]:
        original_range_start: int = range_start
        s3_object_head = self._s3_client.head_object(Bucket=self._bucket_name, Key=self._object_key)

        content_type: str = s3_object_head["ContentType"]
        content_length: int = s3_object_head["ContentLength"]
        shared_logger.debug(
            "get_by_lines",
            extra={
                "content_type": content_type,
                "range_start": range_start,
                "bucket_name": self._bucket_name,
                "object_key": self._object_key,
            },
        )

        if content_type == "application/x-gzip":
            range_start = 0

        if content_type == "application/x-gzip" or original_range_start < content_length:
            s3_object = self._s3_client.get_object(
                Bucket=self._bucket_name, Key=self._object_key, Range=f"bytes={range_start}-"
            )

            for log_event, ending_offset, newline_length in self._generate(
                original_range_start,
                s3_object["Body"],
                content_type,
                content_length,
            ):
                yield log_event, ending_offset, newline_length
        else:
            shared_logger.info(f"requested file content from {range_start}, file size {content_length}: skip it")

    def get_as_string(self) -> str:
        shared_logger.debug("get_as_string", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})
        s3_object = self._s3_client.get_object(Bucket=self._bucket_name, Key=self._object_key, Range="bytes=0-")

        body: StreamingBody = s3_object["Body"]
        return str(body.read(s3_object["ContentLength"]).decode("UTF-8"))
