# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import base64
import binascii
import gzip
from io import SEEK_SET, BytesIO
from typing import Any, Optional

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


class PayloadStorage(CommonStorage):
    """
    PayloadStorage Storage.
    This class implements concrete Payload Storage.
    The payload might be base64 and gzip encoded
    """

    def __init__(
        self,
        payload: str,
        json_content_type: Optional[str] = None,
        multiline_processor: Optional[ProtocolMultiline] = None,
        event_list_from_field_expander: Optional[ExpandEventListFromField] = None,
    ):
        self._payload: str = payload
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
                yield chunk, file_starting_offset, file_ending_offset, b"", None

    def get_by_lines(self, range_start: int) -> GetByLinesIterator:
        original_range_start: int = range_start

        is_gzipped: bool = False
        is_b64encoded: bool = False
        try:
            base64_decoded = base64.b64decode(self._payload, validate=True)
            # we try to unicode decode to catch if `base64.b64decode` decoded to non-valid unicode:
            # in this case `UnicodeDecodeError` will be thrown, this mean that the original was not base64 encoded
            # we try this only if it's not gzipped, because in that case `UnicodeDecodeError` will be thrown anyway
            if not is_gzip_content(base64_decoded):
                base64_decoded.decode("utf-8")
                # if `UnicodeDecodeError` was thrown, the content was not base64 encoded
                # and the below assignment will not be executed
                is_b64encoded = True
            else:
                # we have gzip content that was base64 encoded
                # let's do the proper assignment
                is_b64encoded = True
        except (UnicodeDecodeError, ValueError, binascii.Error):
            # it was not valid unicode base64 encoded value or is it bare gzip content
            # just take as it is and encode to unicode bytes
            base64_decoded = self._payload.encode("utf-8")

        if is_gzip_content(base64_decoded):
            is_gzipped = True
            range_start = 0

        shared_logger.debug(
            "get_by_lines",
            extra={
                "range_start": original_range_start,
                "is_b64encoded": is_b64encoded,
                "is_gzipped": is_gzipped,
            },
        )

        content_length = len(base64_decoded)
        if range_start < content_length:
            file_content: BytesIO = BytesIO(base64_decoded)

            file_content.flush()
            file_content.seek(range_start, SEEK_SET)

            for log_event, line_starting_offset, line_ending_offset, _, event_expanded_offset in self._generate(
                original_range_start, file_content, is_gzipped
            ):
                assert isinstance(log_event, bytes)
                yield log_event, line_starting_offset, line_ending_offset, event_expanded_offset
        else:
            shared_logger.info(f"requested payload content from {range_start}, payload size {content_length}: skip it")

    def get_as_string(self) -> str:
        try:
            base64_decoded = base64.b64decode(self._payload, validate=True)
            if not is_gzip_content(base64_decoded):
                base64_decoded.decode("utf-8")
        except (UnicodeDecodeError, ValueError, binascii.Error):
            base64_decoded = self._payload.encode("utf-8")

        if is_gzip_content(base64_decoded):
            return gzip.decompress(base64_decoded).decode("utf-8")

        return base64_decoded.decode("utf-8")
