from abc import ABCMeta, abstractmethod
from typing import Any


class CommonShipper:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def send(self, event: dict[str, Any]) -> Any:
        pass
