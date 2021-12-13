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
    """
    Shipper factory.
    Provides static methods to instantiate a shipper
    """

    @staticmethod
    def create_from_output(output_type: str, output: Output) -> CommonShipper:
        """
        Instantiates a concrete Shipper given an output type and an Output instance
        """

        if output_type == "elasticsearch":
            if not isinstance(output, ElasticSearchOutput):
                raise ValueError(f"output expected to be ElasticSearchOutput type, given {type(output)}")

            return ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url=output.elasticsearch_url,
                username=output.username,
                password=output.password,
                cloud_id=output.cloud_id,
                api_key=output.api_key,
                dataset=output.dataset,
                namespace=output.namespace,
                tags=output.tags,
            )

        raise ValueError(
            f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
        )

    @staticmethod
    def create(output_type: str, **kwargs: Any) -> CommonShipper:
        """
        Instantiates a concrete Shipper given an output type and the shipper init kwargs
        """

        if output_type not in _init_definition_by_output:
            raise ValueError(
                f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
            )

        output_definition = _init_definition_by_output[output_type]

        output_builder: Callable[..., CommonShipper] = output_definition["class"]

        return output_builder(**kwargs)
