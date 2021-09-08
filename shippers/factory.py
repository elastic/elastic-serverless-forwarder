from shippers.es import ElasticsearchShipper
from shippers.shipper import CommonShipper


class ShipperFactory:
    _init_definition_by_target: dict[str, dict[str, any]] = {
        "elasticsearch": {"class": ElasticsearchShipper, "kwargs": ["hosts", "scheme", "username", "password", "index"]}
    }

    def __init__(self, target: str, **kwargs: any):
        if target not in self._init_definition_by_target:
            raise ValueError(
                f"""
                you must provide one of the following targets:
                    {", ".join(self._init_definition_by_target.keys())}
            """
            )

        init_kwargs: list[str] = [
            key for key in kwargs.keys() if key in self._init_definition_by_target[target]["kwargs"] and kwargs[key]
        ]
        if len(init_kwargs) is not len(self._init_definition_by_target[target]["kwargs"]):
            raise ValueError(
                f"""
                you must provide the following not empty init kwargs for {target}:
                    {", ".join(self._init_definition_by_target[target]["kwargs"])}.
                (provided: {", ".join(kwargs.keys())})
            """
            )

        self._shipper: CommonShipper = self._init_definition_by_target[target]["class"](**kwargs)

    def send(self, event: dict[str, any]) -> any:
        return self._shipper.send(event)
