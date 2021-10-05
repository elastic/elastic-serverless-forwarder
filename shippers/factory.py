# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
from typing import Any, Type

from .es import ElasticsearchShipper
from .shipper import CommonShipper

_init_definition_by_output: dict[str, dict[str, Any]] = {
    "elasticsearch": {
        "class": ElasticsearchShipper,
        "kwargs": ["hosts", "scheme", "username", "password", "dataset", "namespace"],
    }
}


class ShipperFactory:
    @staticmethod
    def create(output: str, **kwargs: Any) -> CommonShipper:
        if output not in _init_definition_by_output:
            raise ValueError(
                f"You must provide one of the following targets: " f"{', '.join(_init_definition_by_output.keys())}"
            )

        output_definition = _init_definition_by_output[output]

        output_kwargs = output_definition["kwargs"]
        output_builder: Type[CommonShipper] = output_definition["class"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in output_kwargs and kwargs[key]]
        if len(init_kwargs) is not len(output_kwargs):
            raise ValueError(
                f"you must provide the following not empty init kwargs for {output}:"
                f" {', '.join(output_kwargs)}. (provided: {json.dumps(kwargs)})"
            )

        return output_builder(**kwargs)
