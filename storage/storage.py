# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Generator


class CommonStorage:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def get_by_lines(self, range_start: int, last_ending_offset: int) -> Generator[tuple[bytes, int, int], None, None]:
        pass

    @abstractmethod
    def get_as_string(self) -> str:
        pass
