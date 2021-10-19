# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
from typing import Any, Callable

from share.config import ElasticSearchOutput, Output

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
    def create_from_output(output_type: str, output: Output) -> CommonShipper:
        if output_type == "elasticsearch":
            elasticsearch_output = ElasticSearchOutput(output.type, output.kwargs)
            return ShipperFactory.create(
                output="elasticsearch",
                hosts=elasticsearch_output.hosts,
                scheme=elasticsearch_output.scheme,
                username=elasticsearch_output.username,
                password=elasticsearch_output.password,
                dataset=elasticsearch_output.dataset,
                namespace=elasticsearch_output.namespace,
            )

        raise ValueError(
            f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
        )

    @staticmethod
    def create(output: str, **kwargs: Any) -> CommonShipper:
        if output not in _init_definition_by_output:
            raise ValueError(
                f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
            )

        output_definition = _init_definition_by_output[output]

        output_kwargs = output_definition["kwargs"]
        output_builder: Callable[..., CommonShipper] = output_definition["class"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in output_kwargs and kwargs[key]]
        if len(init_kwargs) != len(output_kwargs):
            raise ValueError(
                f"you must provide the following not empty init kwargs for {output}:"
                f" {', '.join(output_kwargs)}. (provided: {json.dumps(kwargs)})"
            )

        return output_builder(**kwargs)
