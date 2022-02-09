# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import os
from typing import Any, Callable, Optional

import elasticapm  # noqa: F401
from aws_lambda_typing import context as context_

from share import Config, parse_config, shared_logger
from share.secretsmanager import aws_sm_expander

from .kinesis_trigger import _handle_kinesis_record
from .replay_trigger import _handle_replay_event
from .sqs_trigger import _delete_sqs_record, _handle_sqs_continuation, _handle_sqs_event
from .utils import (
    CONFIG_FROM_PAYLOAD,
    CONFIG_FROM_S3FILE,
    ConfigFileException,
    TriggerTypeException,
    capture_serverless,
    config_yaml_from_payload,
    config_yaml_from_s3,
    get_shipper_and_input,
    get_trigger_type_and_config_source,
    wrap_try_except,
)

_completion_grace_period: int = 120000
_expanders: list[Callable[[str], str]] = [aws_sm_expander]


@capture_serverless
@wrap_try_except
def lambda_handler(lambda_event: dict[str, Any], lambda_context: context_.Context) -> Any:
    """
    AWS Lambda handler in handler.aws package
    Parses the config and acts as front controller for inputs
    """

    try:
        trigger_type, config_source = get_trigger_type_and_config_source(lambda_event)
        shared_logger.info("trigger", extra={"type": trigger_type})
    except Exception as e:
        raise TriggerTypeException(e)

    trigger_event_source_arn: str = lambda_event["Records"][0]["eventSourceARN"]

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

    assert config is not None

    if trigger_type == "replay":
        for replay_record in lambda_event["Records"]:
            event = json.loads(replay_record["body"])
            _handle_replay_event(
                config=config,
                output_type=event["output_type"],
                output_args=event["output_args"],
                event_input_id=event["event_input_id"],
                event_input_type=event["event_input_type"],
                event_payload=event["event_payload"],
            )

            _delete_sqs_record(replay_record["eventSourceARN"], replay_record["receiptHandle"])
            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                shared_logger.info("lambda is going to shutdown")

        return "replayed"

    sent_event: int = 0
    composite_shipper, event_input = get_shipper_and_input(config, config_yaml, trigger_type, lambda_event)

    if trigger_type == "kinesis-data-stream":

        all_sequence_numbers: dict[str, str] = {}
        ret: dict[Any, list[dict[str, str]]] = {"batchItemFailures": []}

        for kinesis_record in lambda_event["Records"]:
            all_sequence_numbers[kinesis_record["kinesis"]["sequenceNumber"]] = kinesis_record["kinesis"][
                "sequenceNumber"
            ]

        for kinesis_record_n, kinesis_record in enumerate(lambda_event["Records"]):
            for es_event, last_ending_offset in _handle_kinesis_record(kinesis_record):
                shared_logger.debug("es_event", extra={"es_event": es_event})

                composite_shipper.send(es_event)
                sent_event += 1

            del all_sequence_numbers[lambda_event["Records"][kinesis_record_n]["kinesis"]["sequenceNumber"]]

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                composite_shipper.flush()

                shared_logger.info(
                    "lambda is going to shutdown, failing unprocessed sequence numbers",
                    extra={"sent_event": sent_event, "sequence_numbers": ", ".join(all_sequence_numbers.keys())},
                )

                for sequence_number in all_sequence_numbers:
                    ret["batchItemFailures"].append({"itemIdentifier": sequence_number})

                return ret

            composite_shipper.flush()
            shared_logger.info("lambda processed all the events", extra={"sent_event": sent_event})

            return ret

    if trigger_type == "s3-sqs":
        previous_sqs_record: int = 0
        for es_event, last_ending_offset, current_sqs_record, current_s3_record in _handle_sqs_event(
            config, lambda_event
        ):
            shared_logger.debug("es_event", extra={"es_event": es_event})

            composite_shipper.send(es_event)
            sent_event += 1

            if current_sqs_record > previous_sqs_record:
                sqs_record = lambda_event["Records"][previous_sqs_record]
                _delete_sqs_record(trigger_event_source_arn, sqs_record["receiptHandle"])

            previous_sqs_record = current_sqs_record

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                sqs_continuing_queue = os.environ["SQS_CONTINUE_URL"]

                shared_logger.info(
                    "lambda is going to shutdown, continuing on dedicated sqs queue",
                    extra={"sqs_queue": sqs_continuing_queue, "sent_event": sent_event},
                )

                composite_shipper.flush()

                _handle_sqs_continuation(
                    trigger_event_source_arn=trigger_event_source_arn,
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
