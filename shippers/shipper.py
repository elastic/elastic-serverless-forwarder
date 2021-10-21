# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Any


class CommonShipper(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    def send(self, event: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        raise NotImplementedError
