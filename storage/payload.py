# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import base64
import binascii
import gzip
from io import SEEK_SET, BytesIO
from typing import Any, Iterator, Optional, Union

from share import shared_logger

from .decorator import JsonCollector, by_lines, inflate
from .storage import CHUNK_SIZE, CommonStorage, StorageReader


class PayloadStorage(CommonStorage):
    """
    PayloadStorage Storage.
    This class implements concrete Payload Storage.
    The payload might be base64 and gzip encoded
    """

    def __init__(self, payload: str):
        self._payload: str = payload

    @JsonCollector
    @by_lines
    @inflate
    def _generate(
        self, range_start: int, body: BytesIO, is_gzipped: bool, content_length: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        """
        Concrete implementation of the iterator for get_by_lines
        """

        file_ending_offset: int = range_start

        def chunk_lambda() -> Any:
            return body.read(CHUNK_SIZE)

        if is_gzipped:
            reader: StorageReader = StorageReader(raw=body)
            yield reader, None, 0, 0, 0
        else:
            for chunk in iter(chunk_lambda, b""):
                file_starting_offset = file_ending_offset
                file_ending_offset += len(chunk)

                shared_logger.debug("_generate flat", extra={"offset": file_ending_offset})
                yield chunk, None, file_ending_offset, file_starting_offset, 0

    def get_by_lines(
        self, range_start: int
    ) -> Iterator[tuple[Union[StorageReader, bytes], Optional[dict[str, Any]], int, int, int]]:
        original_range_start: int = range_start

        try:
            base64_decoded = base64.b64decode(self._payload, validate=True)
        except binascii.Error:
            base64_decoded = self._payload.encode("utf-8")

        is_gzipped: bool = False
        if base64_decoded.startswith(b"\037\213"):  # gzip compression method
            is_gzipped = True
            range_start = 0

        content_length = len(base64_decoded)
        if is_gzipped or original_range_start < content_length:
            file_content: BytesIO = BytesIO(base64_decoded)

            file_content.flush()
            file_content.seek(range_start, SEEK_SET)

            for log_event, json_object, line_ending_offset, line_starting_offset, newline_length in self._generate(
                original_range_start,
                file_content,
                is_gzipped,
                content_length,
            ):
                yield log_event, json_object, line_ending_offset, line_starting_offset, newline_length
        else:
            shared_logger.info(f"requested payload content from {range_start}, payload size {content_length}: skip it")

    def get_as_string(self) -> str:
        shared_logger.debug("get_as_string", extra={"payload": self._payload[0:11]})

        try:
            base64_decoded = base64.b64decode(self._payload, validate=True)
        except binascii.Error:
            base64_decoded = self._payload.encode("utf-8")

        if base64_decoded.startswith(b"\037\213"):  # gzip compression method
            return gzip.decompress(base64_decoded).decode("utf-8")

        return base64_decoded.decode("utf-8")
