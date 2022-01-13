# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import os
from typing import Any, Callable, Optional

import elasticapm  # noqa: F401
from aws_lambda_typing import context as context_

from share import Config, Output, parse_config, shared_logger
from share.secretsmanager import aws_sm_expander
from shippers import CompositeShipper, ElasticsearchShipper, ShipperFactory

from .replay import ReplayEventHandler, _handle_replay_event
from .sqs_trigger import _handle_sqs_continuation, _handle_sqs_event
from .utils import (
    CONFIG_FROM_PAYLOAD,
    CONFIG_FROM_S3FILE,
    ConfigFileException,
    InputConfigException,
    OutputConfigException,
    TriggerTypeException,
    capture_serverless,
    config_yaml_from_payload,
    config_yaml_from_s3,
    get_trigger_type_and_config_source,
    wrap_try_except,
)

_completion_grace_period: int = 120000
_expanders: list[Callable[[str], str]] = [aws_sm_expander]


@capture_serverless
@wrap_try_except
def lambda_handler(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
    """
    AWS Lambda handler in handler.aws package
    Parses the config and acts as front controller for inputs
    """

    try:
        trigger_type, config_source = get_trigger_type_and_config_source(lambda_event)
        shared_logger.info("trigger", extra={"type": trigger_type})
    except Exception as e:
        raise TriggerTypeException(e)

    config_yaml: str = ""
    try:
        if config_source == CONFIG_FROM_PAYLOAD:
            config_yaml = config_yaml_from_payload(lambda_event)

            payload = lambda_event["Records"][0]["messageAttributes"]
            if "originalEventSource" in payload:
                lambda_event["Records"][0]["eventSourceARN"] = payload["originalEventSource"]["stringValue"]
        elif config_source == CONFIG_FROM_S3FILE:
            config_yaml = config_yaml_from_s3()
        else:
            raise ConfigFileException("cannot determine config source")
    except Exception as e:
        raise ConfigFileException(e)

    if config_yaml == "":
        raise ConfigFileException("empty config")

    config: Optional[Config] = None
    try:
        shared_logger.debug("config", extra={"yaml": config_yaml})
        config = parse_config(config_yaml, _expanders)
    except Exception as e:
        raise ConfigFileException(e)

    if trigger_type == "replay":
        event = json.loads(lambda_event["Records"][0]["body"])
        _handle_replay_event(
            config=config,
            output_type=event["output_type"],
            output_args=event["output_args"],
            event_input_id=event["event_input_id"],
            event_input_type=event["event_input_type"],
            event_payload=event["event_payload"],
        )
        return "replayed"

    assert config is not None
    if trigger_type == "sqs" or trigger_type == "self_sqs":
        event_input = config.get_input_by_type_and_id("sqs", lambda_event["Records"][0]["eventSourceARN"])
        if not event_input:
            shared_logger.error(f'no input set for {lambda_event["Records"][0]["eventSourceARN"]}')

            raise InputConfigException("not input set")

        shared_logger.info("input", extra={"type": event_input.type, "id": event_input.id})

        sent_event: int = 0
        composite_shipper: CompositeShipper = CompositeShipper()

        for output_type in event_input.get_output_types():
            if output_type == "elasticsearch":
                shared_logger.info("setting ElasticSearch shipper")
                output: Optional[Output] = event_input.get_output_by_type("elasticsearch")
                if output is None:
                    raise OutputConfigException("no available output for elasticsearch type")

                try:
                    shipper: ElasticsearchShipper = ShipperFactory.create_from_output(
                        output_type="elasticsearch", output=output
                    )
                    shipper.discover_dataset(event=lambda_event)
                    composite_shipper.add_shipper(shipper=shipper)
                    replay_handler = ReplayEventHandler(config_yaml=config_yaml, event_input=event_input)
                    composite_shipper.set_replay_handler(replay_handler=replay_handler.replay_handler)
                except Exception as e:
                    raise OutputConfigException(e)

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
