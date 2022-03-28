# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Any, Callable, TypeVar

ReplayHandlerCallable = Callable[[str, dict[str, Any], dict[str, Any]], None]
EventIdGeneratorCallable = Callable[[dict[str, Any]], str]


class CommonShipper(metaclass=ABCMeta):
    """
    Abstract class for Shipper components
    """

    @abstractmethod
    def __init__(self, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    def send(self, event: dict[str, Any]) -> bool:
        """
        Interface for sending the event by the shipper
        """

        raise NotImplementedError

    @abstractmethod
    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        """
        Interface for setting the event id generator of the shipper
        """

        raise NotImplementedError

    @abstractmethod
    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        """
        Interface for setting the replay handler of the shipper
        """

        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        """
        Interface for flushing the shipper
        """

        raise NotImplementedError


CommonShipperType = TypeVar("CommonShipperType", bound=CommonShipper)
