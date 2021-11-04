# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from typing import Any, Optional

import elasticapm  # noqa: F401
from aws_lambda_typing import context as context_

from share import Config, Output, parse_config, shared_logger
from shippers import CommonShipper, CompositeShipper, ShipperFactory

from .sqs_trigger import _handle_sqs_continuation, _handle_sqs_event
from .utils import capture_serverless, config_yaml_from_payload, config_yaml_from_s3, get_trigger_type, wrap_try_except

_completion_grace_period: int = 60000


@capture_serverless
@wrap_try_except
def lambda_handler(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
    trigger_type: str = get_trigger_type(lambda_event)
    shared_logger.info("trigger", extra={"type": trigger_type})

    config_yaml: str = ""
    if trigger_type == "self_sqs":
        config_yaml = config_yaml_from_payload(lambda_event)
    else:
        config_yaml = config_yaml_from_s3()

    if config_yaml == "":
        shared_logger.error("empty config")
        return "empty config"

    shared_logger.debug("config", extra={"yaml": config_yaml})
    config: Config = parse_config(config_yaml)

    if trigger_type == "sqs" or trigger_type == "self_sqs":
        event_input = config.get_input_by_type_and_id("sqs", lambda_event["Records"][0]["eventSourceARN"])
        if not event_input:
            shared_logger.error(f'no input set for {lambda_event["Records"][0]["eventSourceARN"]}')

            return "not input set"

        shared_logger.info("input", extra={"type": event_input.type, "id": event_input.id})

        sent_event: int = 0
        composite_shipper: CompositeShipper = CompositeShipper()

        for output_type in event_input.get_output_types():
            if output_type == "elasticsearch":
                shared_logger.info("setting ElasticSearch shipper")
                output: Optional[Output] = event_input.get_output_by_type("elasticsearch")
                assert output is not None
                shipper: CommonShipper = ShipperFactory.create_from_output(output_type="elasticsearch", output=output)
                composite_shipper.add_shipper(shipper=shipper)

        for es_event, last_ending_offset, current_sqs_record, current_s3_record in _handle_sqs_event(
            config, lambda_event
        ):
            shared_logger.debug("es_event", extra={"es_event": es_event})

            composite_shipper.send(es_event)
            sent_event += 1

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                sqs_continuing_queue = os.environ["SQS_CONTINUE_URL"]

                shared_logger.info(
                    "lambda is going to shutdown, continuing on dedicated sqs queue",
                    extra={"sqs_queue": sqs_continuing_queue, "sent_event": sent_event},
                )

                composite_shipper.flush()

                _handle_sqs_continuation(
                    sqs_continuing_queue=sqs_continuing_queue,
                    lambda_event=lambda_event,
                    event_input_id=event_input.id,
                    last_ending_offset=last_ending_offset,
                    current_sqs_record=current_sqs_record,
                    current_s3_record=current_s3_record,
                    config_yaml=config_yaml,
                )

                return "continuing"

        composite_shipper.flush()
        shared_logger.info("lambda processed all the events", extra={"sent_event": sent_event})

    return "completed"
