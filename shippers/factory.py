import json
from typing import Type

from .es import ElasticsearchShipper
from .shipper import CommonShipper

_init_definition_by_target: dict[str, dict[str, any]] = {
    "elasticsearch": {
        "class": ElasticsearchShipper,
        "kwargs": ["hosts", "scheme", "username", "password", "dataset", "namespace"],
    }
}


class ShipperFactory:
    @staticmethod
    def create(target: str, **kwargs: any) -> CommonShipper:
        if target not in _init_definition_by_target:
            raise ValueError(
                f"""
                you must provide one of the following targets:
                    {", ".join(_init_definition_by_target.keys())}
            """
            )

        target_definition = _init_definition_by_target[target]

        target_kwargs = target_definition["kwargs"]
        target_builder: Type[CommonShipper] = target_definition["class"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in target_kwargs and kwargs[key]]
        if len(init_kwargs) is not len(target_kwargs):
            raise ValueError(
                f"""
                you must provide the following not empty init kwargs for {target}:
                    {", ".join(target_kwargs)}.
                (provided: {json.dumps(kwargs)})
            """
            )

        return target_builder(**kwargs)
