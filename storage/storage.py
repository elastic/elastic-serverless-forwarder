from abc import ABCMeta, abstractmethod
from typing import Generator


class CommonStorage:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def get(self) -> Generator[tuple[bytes, int], None, None]:
        pass
