from abc import ABCMeta, abstractmethod
from typing import Generator


class CommonStorage:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def get_by_lines(
        self, range_start: int, last_beginning_offset: int
    ) -> Generator[tuple[bytes, int, int], None, None]:
        pass

    @abstractmethod
    def get_as_string(self) -> str:
        pass
