# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Optional

from share import IncludeExcludeFilter

from .shipper import CommonShipper, EventIdGeneratorCallable, ReplayHandlerCallable


class CompositeShipper(CommonShipper):
    """
    Composite Shipper.
    This class implements composite pattern for shippers
    """

    def __init__(self, **kwargs: Any):
        self._shippers: list[CommonShipper] = []
        self._include_exclude_filter: Optional[IncludeExcludeFilter] = None

    def add_include_exclude_filter(self, include_exclude_filter: Optional[IncludeExcludeFilter]) -> None:
        """
        IncludeExcludeFilter setter.
        Add an includeExcludeFilter to the composite
        """
        self._include_exclude_filter = include_exclude_filter

    def add_shipper(self, shipper: CommonShipper) -> None:
        """
        Shipper setter.
        Add a shipper to the composite
        """
        self._shippers.append(shipper)

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        for shipper in self._shippers:
            shipper.set_event_id_generator(event_id_generator=event_id_generator)

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        for shipper in self._shippers:
            shipper.set_replay_handler(replay_handler=replay_handler)

    def send(self, event: dict[str, Any]) -> Any:
        if self._include_exclude_filter is not None and not self._include_exclude_filter.filter(event):
            return

        for shipper in self._shippers:
            shipper.send(event)

    def flush(self) -> None:
        for shipper in self._shippers:
            shipper.flush()
