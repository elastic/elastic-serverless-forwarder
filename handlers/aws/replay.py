# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import os
from typing import Any, Optional

from share import Config, ElasticsearchOutput, Input, Output, shared_logger
from shippers import ShipperFactory

from .utils import InputConfigException, OutputConfigException, get_sqs_client


class ReplayEventHandler:
    def __init__(self, config_yaml: str, event_input: Input):
        self._config_yaml: str = config_yaml
        self._event_input_id: str = event_input.id
        self._event_input_type: str = event_input.type

    def replay_handler(self, output_type: str, output_args: dict[str, Any], event_payload: dict[str, Any]) -> None:
        sqs_replay_queue = os.environ["SQS_REPLAY_URL"]

        sqs_client = get_sqs_client()

        message_payload: dict[str, Any] = {
            "output_type": output_type,
            "output_args": output_args,
            "event_payload": event_payload,
            "event_input_id": self._event_input_id,
            "event_input_type": self._event_input_type,
        }

        sqs_client.send_message(
            QueueUrl=sqs_replay_queue,
            MessageBody=json.dumps(message_payload),
            MessageAttributes={
                "config": {"StringValue": self._config_yaml, "DataType": "String"},
            },
        )

        shared_logger.warning("sent to replay queue", extra=message_payload)


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
        output.dataset = output_args["dataset"]
        shared_logger.info("setting ElasticSearch shipper")
        elasticsearch = ShipperFactory.create_from_output(output_type=output_type, output=output)

        elasticsearch.send(event_payload)
        elasticsearch.flush()
