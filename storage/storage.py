# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta
from io import BytesIO
from typing import Any, Callable, Iterator, Optional, Protocol, TypeVar, Union

from typing_extensions import TypeAlias

from share import ExpandEventListFromField, ProtocolMultiline

# CHUNK_SIZE is how much we read from the gzip stream at every iteration in the inflate decorator
# BEWARE, this CHUNK_SIZE has a huge impact on performance, contrary to what we stated here:
# https://github.com/elastic/elastic-serverless-forwarder/pull/11#discussion_r732587976
# Reinstating to 1M from 1K resulted on 6.2M gzip of 35.1 of inflated content
# to be ingested in 45 secs instead of having the lambda timing out
CHUNK_SIZE: int = 1024**2


def is_gzip_content(content: bytes) -> bool:
    return content.startswith(b"\037\213")  # gzip compression method


class StorageReader:
    """
    StorageReader is an interface for contents returned by storage.
    It wraps the underlying type and forward to it
    """

    def __init__(self, raw: Any):
        self._raw = raw

    def __getattr__(self, item: str) -> Any:
        return getattr(self._raw, item)


# GetByLinesIterator yields a tuple of content, starting offset, ending offset
# and optional offset of a list of expanded events
GetByLinesIterator: TypeAlias = Iterator[tuple[bytes, int, int, Optional[int]]]


class ProtocolStorage(Protocol):
    """
    Protocol for Storage components
    """

    json_content_type: Optional[str]
    multiline_processor: Optional[ProtocolMultiline]
    event_list_from_field_expander: Optional[ExpandEventListFromField]

    def get_by_lines(self, range_start: int) -> GetByLinesIterator:
        pass  # pragma: no cover

    def get_as_string(self) -> str:
        pass  # pragma: no cover


class CommonStorage(metaclass=ABCMeta):
    """
    Common class for Storage components
    """

    json_content_type: Optional[str] = None
    multiline_processor: Optional[ProtocolMultiline] = None
    event_list_from_field_expander: Optional[ExpandEventListFromField] = None


ProtocolStorageType = TypeVar("ProtocolStorageType", bound=ProtocolStorage)

# StorageDecoratorIterator yields a tuple of content (expressed as `StorageReader` or bytes), starting offset,
# ending offset, newline and optional offset of a list of expanded events
StorageDecoratorIterator: TypeAlias = Iterator[tuple[Union[StorageReader, bytes], int, int, bytes, Optional[int]]]

# StorageDecoratorCallable accepts a `ProtocolStorageType`, the range start offset, the content as BytesIO and a boolean
# flag indicating if the content is gzipped as arguments. It returns a `StorageDecoratorIterator`
StorageDecoratorCallable = Callable[[ProtocolStorageType, int, BytesIO, bool], StorageDecoratorIterator]
