# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import Any, Callable, Iterator, TypeVar, Union

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


class CommonStorage(metaclass=ABCMeta):
    """
    Abstract class for Storage components
    """

    @abstractmethod
    def __init__(self, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    def get_by_lines(self, range_start: int) -> Iterator[tuple[Union[StorageReader, bytes], int, int]]:
        """
        Interface for getting content from storage line by line.
        Decorators defining the specific meaning of "line" will be applied in concrete implementations.
        """

        raise NotImplementedError

    @abstractmethod
    def get_as_string(self) -> str:
        """
        Interface for getting content from storage as string.
        """

        raise NotImplementedError


CommonStorageType = TypeVar("CommonStorageType", bound=CommonStorage)
GetByLinesCallable = Callable[
    [CommonStorageType, int, BytesIO, str, int], Iterator[tuple[Union[StorageReader, bytes], int, int]]
]
