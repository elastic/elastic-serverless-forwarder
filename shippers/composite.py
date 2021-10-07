# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any

from .shipper import CommonShipper


class CompositeShipper(CommonShipper):
    def __init__(self, **kwargs: Any):
        self._shippers: list[CommonShipper] = []

    def add_shipper(self, shipper: CommonShipper) -> None:
        self._shippers.append(shipper)

    def send(self, event: dict[str, Any]) -> Any:
        for shipper in self._shippers:
            shipper.send(event)

    def flush(self) -> None:
        for shipper in self._shippers:
            shipper.flush()
