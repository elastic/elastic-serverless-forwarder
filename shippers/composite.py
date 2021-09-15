from typing import Any

from .shipper import CommonShipper


class CompositeShipper(CommonShipper):
    def __init__(self, **kwargs):
        self._shippers: list[CommonShipper] = []

    def add_shipper(self, shipper: CommonShipper):
        self._shippers.append(shipper)

    def send(self, event: dict[str, Any]) -> Any:
        for shipper in self._shippers:
            shipper.send(event)

    def flush(self):
        for shipper in self._shippers:
            shipper.flush()
