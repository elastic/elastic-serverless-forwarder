# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta
from io import BytesIO
from typing import Any, Callable, Iterator, Optional, Protocol, TypeVar, Union

from share import ExpandEventListFromField, ProtocolMultiline

CHUNK_SIZE: int = 1024


class StorageReader:
    """
    StorageReader is an interface for contents returned by storage.
    It wraps the underlying type and forward to it
    """

    def __init__(self, raw: Any):
        self._raw = raw

    def __getattr__(self, item: str) -> Any:
        return getattr(self._raw, item)


class ProtocolStorage(Protocol):
    """
    Protocol for Storage components
    """

    multiline_processor: Optional[ProtocolMultiline]
    expand_event_list_from_field: Optional[ExpandEventListFromField]

    def get_by_lines(self, range_start: int) -> Iterator[tuple[bytes, int, int, int]]:
        pass  # pragma: no cover

    def get_as_string(self) -> str:
        pass  # pragma: no cover


class CommonStorage(metaclass=ABCMeta):
    """
    Common class for Storage components
    """

    multiline_processor: Optional[ProtocolMultiline] = None
    expand_event_list_from_field: Optional[ExpandEventListFromField] = None


ProtocolStorageType = TypeVar("ProtocolStorageType", bound=ProtocolStorage)
GetByLinesCallable = Callable[
    [ProtocolStorageType, int, BytesIO, bool], Iterator[tuple[Union[StorageReader, bytes], int, int, int, int]]
]
