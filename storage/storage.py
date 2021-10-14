# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Any, Iterator, TypeVar


class CommonStorage(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, **kwargs: Any):
        pass

    @abstractmethod
    def get_by_lines(self, range_start: int, last_ending_offset: int) -> Iterator[tuple[bytes, int, int]]:
        pass

    @abstractmethod
    def get_as_string(self) -> str:
        pass


CommonStorageType = TypeVar("CommonStorageType", bound=CommonStorage)
