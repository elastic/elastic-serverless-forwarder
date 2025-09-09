# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class ProcessorResult:
    def __init__(self, events: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None) -> None:
        if events is None:
            self.events: List[Dict[str, Any]] = []
        elif isinstance(events, dict):
            self.events = [events]
        elif isinstance(events, list):
            self.events = events
        else:
            raise ValueError("events must be None, a dict, or a list of dicts")

    def __len__(self) -> int:
        return len(self.events)

    @property
    def is_empty(self) -> bool:
        return len(self.events) == 0

    def to_dict(self) -> dict[str, Any]:
        if self.is_empty:
            return {}
        elif len(self.events) == 1:
            return self.events[0]
        else:
            return {str(k): v for k, v in enumerate(self.events)}


class BaseProcessor(ABC):

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}

    def configure(self, config: Dict[str, Any]) -> None:
        self._config = config.copy()

    @abstractmethod
    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        """
        Process an event and return the result.

        Args:
            event: The event to process
            context: Optional context information about the event source

        Returns:
            ProcessorResult containing zero, one, or multiple processed events
        """
        pass
