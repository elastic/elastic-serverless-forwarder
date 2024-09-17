# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable

from share.config import ElasticsearchOutput, LogstashOutput, Output

from .es import ElasticsearchShipper
from .logstash import LogstashShipper
from .shipper import ProtocolShipper

_init_definition_by_output: dict[str, dict[str, Any]] = {
    "elasticsearch": {
        "class": ElasticsearchShipper,
    },
    "logstash": {
        "class": LogstashShipper,
    },
}


class ShipperFactory:
    """
    Shipper factory.
    Provides static methods to instantiate a shipper
    """

    @staticmethod
    def create_from_output(output_type: str, output: Output) -> ProtocolShipper:
        """
        Instantiates a concrete Shipper given an output type and an Output instance
        """

        if output_type == "elasticsearch":
            if not isinstance(output, ElasticsearchOutput):
                raise ValueError(f"output expected to be ElasticsearchOutput type, given {type(output)}")

            return ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url=output.elasticsearch_url,
                username=output.username,
                password=output.password,
                cloud_id=output.cloud_id,
                api_key=output.api_key,
                es_datastream_name=output.es_datastream_name,
                tags=output.tags,
                batch_max_actions=output.batch_max_actions,
                batch_max_bytes=output.batch_max_bytes,
                ssl_assert_fingerprint=output.ssl_assert_fingerprint,
                es_dead_letter_index=output.es_dead_letter_index,
            )

        if output_type == "logstash":
            if not isinstance(output, LogstashOutput):
                raise ValueError(f"output expected to be LogstashOutput type, given {type(output)}")

            return ShipperFactory.create(
                output_type="logstash",
                logstash_url=output.logstash_url,
                username=output.username,
                password=output.password,
                max_batch_size=output.max_batch_size,
                compression_level=output.compression_level,
                ssl_assert_fingerprint=output.ssl_assert_fingerprint,
                tags=output.tags,
            )

        raise ValueError(
            f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
        )

    @staticmethod
    def create(output_type: str, **kwargs: Any) -> ProtocolShipper:
        """
        Instantiates a concrete Shipper given an output type and the shipper init kwargs
        """

        if output_type not in _init_definition_by_output:
            raise ValueError(
                f"You must provide one of the following outputs: " f"{', '.join(_init_definition_by_output.keys())}"
            )

        output_definition = _init_definition_by_output[output_type]

        output_builder: Callable[..., ProtocolShipper] = output_definition["class"]

        return output_builder(**kwargs)
