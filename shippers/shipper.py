from abc import ABCMeta, abstractmethod


class CommonShipper:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def send(self, event: dict[str, any]) -> any:
        pass
