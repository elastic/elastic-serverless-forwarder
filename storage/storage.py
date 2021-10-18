# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Iterator, TypeVar


class CommonStorage(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    def get_by_lines(self, range_start: int) -> Iterator[tuple[bytes, int, int]]:
        raise NotImplementedError

    @abstractmethod
    def get_as_string(self) -> str:
        raise NotImplementedError


CommonStorageType = TypeVar("CommonStorageType", bound=CommonStorage)
GetByLinesCallable = Callable[[CommonStorageType, int, Any, str, int], Iterator[tuple[bytes, int, int]]]
