from typing import Generator

import boto3
from botocore.response import StreamingBody

from share import by_line, deflate
from storage.storage import CommonStorage


class S3Storage(CommonStorage):
    _chunk_size: int = 1024 * 1024

    def __init__(self, bucket_arn: str, object_key: str):
        self._bucket_arn: str = bucket_arn
        self._object_key: str = object_key

        # Get the service resource
        self._s3_client = boto3.client("s3")

    @by_line
    @deflate
    def _generate(self, body: StreamingBody, content_type: str) -> Generator[tuple[bytes, int], None, None]:
        for chunk in iter(lambda: body.read(self._chunk_size), b""):
            yield chunk, len(chunk)

    def get(self) -> Generator[tuple[bytes, int], None, None]:
        print(self._bucket_arn, self._object_key)
        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_arn,
            Key=self._object_key,
        )

        return self._generate(s3_object["Body"], s3_object["ContentType"])
