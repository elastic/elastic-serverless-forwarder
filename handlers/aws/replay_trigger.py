# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Optional

from share import Config, ElasticsearchOutput, Input, LogstashOutput, Output, shared_logger
from shippers import CompositeShipper, ProtocolShipper, ShipperFactory

from .exceptions import InputConfigException, OutputConfigException, ReplayHandlerException
from .utils import delete_sqs_record


class ReplayedEventReplayHandler:
    def __init__(self, replay_queue_arn: str):
        self._replay_queue_arn = replay_queue_arn
        self._failed_event_ids: list[str] = []
        self._events_with_receipt_handle: dict[str, str] = {}

    def add_event_with_receipt_handle(self, event_uniq_id: str, receipt_handle: str) -> None:
        self._events_with_receipt_handle[event_uniq_id] = receipt_handle

    def replay_handler(
        self, output_destination: str, output_args: dict[str, Any], event_payload: dict[str, Any]
    ) -> None:
        event_uniq_id: str = event_payload["_id"] + output_destination
        self._failed_event_ids.append(event_uniq_id)

    def flush(self) -> None:
        for failed_event_uniq_id in self._failed_event_ids:
            del self._events_with_receipt_handle[failed_event_uniq_id]

        for receipt_handle in self._events_with_receipt_handle.values():
            delete_sqs_record(self._replay_queue_arn, receipt_handle)

        if len(self._failed_event_ids) > 0:
            raise ReplayHandlerException()


def get_shipper_for_replay_event(
    config: Config,
    output_destination: str,
    output_args: dict[str, Any],
    event_input_id: str,
    replay_handler: ReplayedEventReplayHandler,
) -> Optional[CompositeShipper]:
    event_input: Optional[Input] = config.get_input_by_id(event_input_id)
    if event_input is None:
        raise InputConfigException(f"Cannot load input for input id {event_input_id}")

    output: Optional[Output] = event_input.get_output_by_destination(output_destination)
    if output is None:
        raise OutputConfigException(f"Cannot load output with destination {output_destination}")

    # Let's wrap the specific output shipper in the composite one, since the composite deepcopy the mutating events
    shipper: CompositeShipper = CompositeShipper()

    if output.type == "elasticsearch":
        assert isinstance(output, ElasticsearchOutput)
        output.es_datastream_name = output_args["es_datastream_name"]
        shared_logger.debug("setting ElasticSearch shipper")
        elasticsearch: ProtocolShipper = ShipperFactory.create_from_output(output_type=output.type, output=output)

        shipper.add_shipper(elasticsearch)
        shipper.set_replay_handler(replay_handler=replay_handler.replay_handler)

        return shipper

    if output.type == "logstash":
        assert isinstance(output, LogstashOutput)
        shared_logger.debug("setting Logstash shipper")
        logstash: ProtocolShipper = ShipperFactory.create_from_output(output_type=output.type, output=output)

        shipper.add_shipper(logstash)
        shipper.set_replay_handler(replay_handler=replay_handler.replay_handler)

        return shipper

    return None
