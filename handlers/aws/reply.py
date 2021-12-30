# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any

from share import ElasticsearchOutput, shared_logger
from shippers import ShipperFactory


def _handle_reply_event(output_type: str, output_args: dict[str, Any], payload: dict[str, Any]) -> None:
    if output_type == "elasticsearch":
        shared_logger.info("setting ElasticSearch shipper")
        output = ElasticsearchOutput(**output_args)
        elasticsearch = ShipperFactory.create_from_output(output_type=output_type, output=output)

        elasticsearch.send(payload)
        elasticsearch.flush()
