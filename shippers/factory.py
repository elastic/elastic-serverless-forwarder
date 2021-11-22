# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable

from share.config import ElasticSearchOutput, Output

from .es import ElasticsearchShipper
from .shipper import CommonShipper

_init_definition_by_output: dict[str, dict[str, Any]] = {
    "elasticsearch": {
        "class": ElasticsearchShipper,
    }
}


class ShipperFactory:
    @staticmethod
    def create_from_output(output_type: str, output: Output) -> CommonShipper:
        if output_type == "elasticsearch":
            if not isinstance(output, ElasticSearchOutput):
                raise ValueError(f"output expected to be ElasticSearchOutput type, given {type(output)}")

            return ShipperFactory.create(
                output="elasticsearch",
                elasticsearch_url=output.elasticsearch_url,
                username=output.username,
                password=output.password,
                cloud_id=output.cloud_id,
                api_key=output.api_key,
                dataset=output.dataset,
                namespace=output.namespace,
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

        output_builder: Callable[..., CommonShipper] = output_definition["class"]

        return output_builder(**kwargs)
