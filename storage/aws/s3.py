import boto3
from typing import Generator
from botocore.response import StreamingBody
from utils.data import by_line, deflate

from storage.common.commonstorage import CommonStorage


class S3Storage(CommonStorage):
    _chunk_size: int = 1024 * 1024
    _max_number_of_messages: int = 10

    def __init__(self, bucket_arn: str, object_key: str):
        self._bucket_arn: str = bucket_arn
        self._object_key: str = object_key

        # Get the service resource
        self._s3_client = boto3.resource('s3')

    @by_line
    @deflate
    def _generate(self, body: StreamingBody, content_type: str) -> Generator[(bytes, int), None, None]:
        for chunk in iter(lambda: body.read(self._chunk_size), b''):
            yield chunk, len(chunk)

    def get(self) -> Generator[(bytes, int), None, None]:
        s3_object = self._s3_client.get_object(
            Bucket=self._bucket_arn,
            Key=self._object_key,
        )

        return self._generate(s3_object["Body"], s3_object["ContentType"])
