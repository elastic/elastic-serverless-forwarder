# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Optional

from share import Config, ElasticsearchOutput, Input, Output, shared_logger
from shippers import ElasticsearchShipper, ShipperFactory

from .utils import InputConfigException, OutputConfigException


def _handle_replay_event(
    config: Config,
    output_type: str,
    output_args: dict[str, Any],
    event_input_id: str,
    event_input_type: str,
    event_payload: dict[str, Any],
) -> None:

    event_input: Optional[Input] = config.get_input_by_type_and_id(event_input_type, event_input_id)
    if event_input is None:
        raise InputConfigException(f"cannot load input for type {event_input_type} with id {event_input_id}")

    output: Optional[Output] = event_input.get_output_by_type(output_type)
    if output is None:
        raise OutputConfigException(f"cannot load output of type {output_type}")

    if output_type == "elasticsearch":
        assert isinstance(output, ElasticsearchOutput)
        output.es_index_or_datastream_name = output_args["es_index_or_datastream_name"]
        shared_logger.info("setting ElasticSearch shipper")
        elasticsearch: ElasticsearchShipper = ShipperFactory.create_from_output(output_type=output_type, output=output)
        elasticsearch.send(event_payload)
        elasticsearch.flush()
