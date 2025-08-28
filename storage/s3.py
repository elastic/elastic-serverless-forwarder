# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from io import SEEK_SET, BytesIO
from typing import Any, Optional

import boto3
import botocore.client
import elasticapm  # noqa: F401
from botocore.response import StreamingBody

from share import ExpandEventListFromField, ProtocolMultiline, shared_logger

from .decorator import by_lines, inflate, json_collector, multi_line
from .storage import (
    CHUNK_SIZE,
    CommonStorage,
    GetByLinesIterator,
    StorageDecoratorIterator,
    StorageReader,
    is_gzip_content,
)


class S3Storage(CommonStorage):
    """
    S3 Storage.
    This class implements concrete S3 Storage
    """

    _s3_client = boto3.client(
        "s3", config=botocore.client.Config(retries={"total_max_attempts": 10, "mode": "standard"})
    )

    def __init__(
        self,
        bucket_name: str,
        object_key: str,
        json_content_type: Optional[str] = None,
        multiline_processor: Optional[ProtocolMultiline] = None,
        event_list_from_field_expander: Optional[ExpandEventListFromField] = None,
    ):
        self._bucket_name: str = bucket_name
        self._object_key: str = object_key
        self.json_content_type = json_content_type
        self.multiline_processor = multiline_processor
        self.event_list_from_field_expander = event_list_from_field_expander

    @multi_line
    @json_collector
    @by_lines
    @inflate
    def _generate(self, range_start: int, body: BytesIO, is_gzipped: bool) -> StorageDecoratorIterator:
        """
        Concrete implementation of the iterator for get_by_lines
        """

        file_ending_offset: int = range_start

        def chunk_lambda() -> Any:
            return body.read(CHUNK_SIZE)

        if is_gzipped:
            reader: StorageReader = StorageReader(raw=body)
            yield reader, 0, 0, b"", None
        else:
            for chunk in iter(chunk_lambda, b""):
                file_starting_offset = file_ending_offset
                file_ending_offset += len(chunk)

                shared_logger.debug("_generate flat", extra={"offset": file_ending_offset})
                yield chunk, file_ending_offset, file_starting_offset, b"", None

    def get_by_lines(self, range_start: int, binary_processor_type: Optional[str] = None) -> GetByLinesIterator:
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
                "binary_processor_type": binary_processor_type,
            },
        )

        file_content: BytesIO = BytesIO(b"")
        self._s3_client.download_fileobj(self._bucket_name, self._object_key, file_content)

        file_content.flush()
        file_content.seek(0, SEEK_SET)
        is_gzipped: bool = False
        if is_gzip_content(file_content.readline()):
            is_gzipped = True
            range_start = 0

        if range_start < content_length:
            file_content.seek(range_start, SEEK_SET)

            # If binary_processor_type is set to 'ipfix', decode using IPFIXProcessor
            if binary_processor_type == "ipfix":
                from processors.ipfix import IPFIXProcessor
                import json
                # Use BytesIO for IPFIXProcessor
                file_content.seek(0)

                # If file is gzipped, we need to decompress it first
                if is_gzipped:
                    import gzip
                    try:
                        file_content.seek(0)
                        with gzip.GzipFile(fileobj=file_content, mode='rb') as gz_file:
                            decompressed_data = gz_file.read()
                        shared_logger.debug(
                            "Successfully decompressed IPFIX file",
                            extra={
                                "original_size": content_length,
                                "decompressed_size": len(decompressed_data)
                            }
                        )
                    except Exception as e:
                        shared_logger.error(
                            "Failed to decompress IPFIX file",
                            extra={"error": str(e), "bucket": self._bucket_name, "key": self._object_key}
                        )
                        raise
                else:
                    decompressed_data = file_content.getvalue()

                processor = IPFIXProcessor()
                # Simulate event dict for processor
                event = {"body": decompressed_data}

                try:
                    result = processor.process(event)
                    # Yield each record from the result as JSON bytes
                    if not result.is_empty:
                        for record in result.events:
                            # Convert the record to JSON bytes to maintain compatibility
                            json_bytes = json.dumps(record).encode('utf-8')
                            # Use 0 for offsets since binary decoding doesn't use them meaningfully
                            yield json_bytes, 0, 0, None
                    else:
                        shared_logger.warning(
                            "No IPFIX records found in file",
                            extra={"bucket": self._bucket_name, "key": self._object_key}
                        )
                except Exception as e:
                    shared_logger.error(
                        "Error processing IPFIX file",
                        extra={
                            "error": str(e),
                            "bucket": self._bucket_name,
                            "key": self._object_key,
                            "file_size": len(decompressed_data)
                        }
                    )
                    raise
            else:
                for log_event, line_starting_offset, line_ending_offset, _, event_expanded_offset in self._generate(
                    original_range_start, file_content, is_gzipped
                ):
                    assert isinstance(log_event, bytes)
                    yield log_event, line_starting_offset, line_ending_offset, event_expanded_offset
        else:
            shared_logger.info(f"requested file content from {range_start}, file size {content_length}: skip it")

    def get_as_string(self) -> str:
        shared_logger.debug("get_as_string", extra={"bucket_name": self._bucket_name, "object_key": self._object_key})
        s3_object = self._s3_client.get_object(Bucket=self._bucket_name, Key=self._object_key, Range="bytes=0-")

        body: StreamingBody = s3_object["Body"]
        return str(body.read(s3_object["ContentLength"]).decode("utf-8"))
