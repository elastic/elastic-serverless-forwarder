# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import os
from typing import Any, Callable, Optional

from aws_lambda_typing import context as context_

from share import parse_config, shared_logger
from share.secretsmanager import aws_sm_expander
from shippers import EVENT_IS_FILTERED, EVENT_IS_SENT, CompositeShipper

from .cloudwatch_logs_trigger import (
    _from_awslogs_data_to_event,
    _handle_cloudwatch_logs_continuation,
    _handle_cloudwatch_logs_event,
)
from .kinesis_trigger import _handle_kinesis_record
from .replay_trigger import _handle_replay_event
from .s3_sqs_trigger import _handle_s3_sqs_continuation, _handle_s3_sqs_event
from .sqs_trigger import _handle_sqs_continuation, _handle_sqs_event
from .utils import (
    CONFIG_FROM_PAYLOAD,
    CONFIG_FROM_S3FILE,
    ConfigFileException,
    TriggerTypeException,
    capture_serverless,
    config_yaml_from_payload,
    config_yaml_from_s3,
    delete_sqs_record,
    discover_integration_scope,
    get_log_group_arn_and_region_from_log_group_name,
    get_shipper_from_input,
    get_sqs_client,
    get_trigger_type_and_config_source,
    is_continuing_of_cloudwatch_logs,
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

    shared_logger.debug(
        "lambda triggered", extra={"event": lambda_event, "invoked_function_arn": lambda_context.invoked_function_arn}
    )

    try:
        trigger_type, config_source = get_trigger_type_and_config_source(lambda_event)
        shared_logger.info("trigger", extra={"type": trigger_type})
    except Exception as e:
        raise TriggerTypeException(e)

    try:
        if config_source == CONFIG_FROM_PAYLOAD:
            config_yaml = config_yaml_from_payload(lambda_event)
        elif config_source == CONFIG_FROM_S3FILE:
            config_yaml = config_yaml_from_s3()
        else:
            raise ConfigFileException("cannot determine config source")
    except Exception as e:
        raise ConfigFileException(e)

    if config_yaml == "":
        raise ConfigFileException("empty config")

    try:
        shared_logger.debug("config", extra={"yaml": config_yaml})
        config = parse_config(config_yaml, _expanders, discover_integration_scope)
    except Exception as e:
        raise ConfigFileException(e)

    assert config is not None

    sqs_client = get_sqs_client()

    if trigger_type == "replay-sqs":
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

            delete_sqs_record(replay_record["eventSourceARN"], replay_record["receiptHandle"])
            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                shared_logger.info("lambda is going to shutdown")

        return "replayed"

    sent_events: int = 0
    empty_events: int = 0
    skipped_events: int = 0

    if trigger_type == "cloudwatch-logs":
        cloudwatch_logs_event = _from_awslogs_data_to_event(lambda_event["event"]["awslogs"]["data"])
        log_group_arn, aws_region = get_log_group_arn_and_region_from_log_group_name(cloudwatch_logs_event["logGroup"])
        input_id = log_group_arn

        if "logGroup" not in cloudwatch_logs_event:
            raise ValueError("logGroup key not present in the cloudwatch logs payload")

        if "logStream" not in cloudwatch_logs_event:
            raise ValueError("logStream key not present in the cloudwatch logs payload")

        event_input = config.get_input_by_type_and_id(trigger_type, input_id)
        if not event_input:
            shared_logger.warning("no input defined", extra={"input_type": trigger_type, "input_id": input_id})

            return "completed"

        composite_shipper = get_shipper_from_input(
            event_input=event_input, lambda_event=lambda_event, at_record=0, config_yaml=config_yaml
        )

        for es_event, last_ending_offset, current_log_event_n in _handle_cloudwatch_logs_event(
            cloudwatch_logs_event, aws_region
        ):
            shared_logger.debug("es_event", extra={"es_event": es_event})

            sent_outcome = composite_shipper.send(es_event)
            if sent_outcome == EVENT_IS_SENT:
                sent_events += 1
            elif sent_outcome == EVENT_IS_FILTERED:
                skipped_events += 1
            else:
                empty_events += 1

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                sqs_continuing_queue = os.environ["SQS_CONTINUE_URL"]

                shared_logger.info(
                    "lambda is going to shutdown, continuing on dedicated sqs queue",
                    extra={
                        "sqs_queue": sqs_continuing_queue,
                        "sent_events": sent_events,
                        "empty_events": empty_events,
                        "skipped_events": skipped_events,
                    },
                )

                composite_shipper.flush()

                _handle_cloudwatch_logs_continuation(
                    sqs_client=sqs_client,
                    sqs_continuing_queue=sqs_continuing_queue,
                    last_ending_offset=last_ending_offset,
                    cloudwatch_logs_event=cloudwatch_logs_event,
                    current_log_event=current_log_event_n,
                    event_input_id=input_id,
                    config_yaml=config_yaml,
                )

                return "continuing"

        composite_shipper.flush()
        shared_logger.info(
            "lambda processed all the events",
            extra={"sent_event": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
        )

    if trigger_type == "kinesis-data-stream":
        all_sequence_numbers: dict[str, str] = {}
        ret: dict[Any, list[dict[str, str]]] = {"batchItemFailures": []}

        input_id = lambda_event["Records"][0]["eventSourceARN"]
        event_input = config.get_input_by_type_and_id(trigger_type, input_id)
        if not event_input:
            shared_logger.warning("no input defined", extra={"input_type": trigger_type, "input_id": input_id})

            return "completed"

        composite_shipper = get_shipper_from_input(
            event_input=event_input, lambda_event=lambda_event, at_record=0, config_yaml=config_yaml
        )

        for kinesis_record in lambda_event["Records"]:
            all_sequence_numbers[kinesis_record["kinesis"]["sequenceNumber"]] = kinesis_record["kinesis"][
                "sequenceNumber"
            ]

        for kinesis_record_n, kinesis_record in enumerate(lambda_event["Records"]):
            for es_event, last_ending_offset in _handle_kinesis_record(kinesis_record):
                shared_logger.debug("es_event", extra={"es_event": es_event})

                sent_outcome = composite_shipper.send(es_event)
                if sent_outcome == EVENT_IS_SENT:
                    sent_events += 1
                elif sent_outcome == EVENT_IS_FILTERED:
                    skipped_events += 1
                else:
                    empty_events += 1

            del all_sequence_numbers[lambda_event["Records"][kinesis_record_n]["kinesis"]["sequenceNumber"]]

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                composite_shipper.flush()

                shared_logger.info(
                    "lambda is going to shutdown, failing unprocessed sequence numbers",
                    extra={
                        "sent_events": sent_events,
                        "empty_events": empty_events,
                        "skipped_events": skipped_events,
                        "sequence_numbers": ", ".join(all_sequence_numbers.keys()),
                    },
                )

                for sequence_number in all_sequence_numbers:
                    ret["batchItemFailures"].append({"itemIdentifier": sequence_number})

                return ret

            composite_shipper.flush()
            shared_logger.info(
                "lambda processed all the events",
                extra={"sent_event": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
            )

            return ret

    if trigger_type == "s3-sqs" or trigger_type == "sqs":
        composite_shipper_cache: dict[str, CompositeShipper] = {}

        def event_processing(
            processing_composing_shipper: CompositeShipper,
            processing_es_event: dict[str, Any],
        ) -> tuple[bool, str]:
            shared_logger.debug("es_event", extra={"es_event": processing_es_event})

            processing_sent_outcome = processing_composing_shipper.send(processing_es_event)

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                return True, processing_sent_outcome

            return False, processing_sent_outcome

        def handle_timeout(
            remaining_sqs_records: list[dict[str, Any]],
            timeout_last_ending_offset: Optional[int],
            timeout_sent_events: int,
            timeout_empty_events: int,
            timeout_skipped_events: int,
            timeout_config_yaml: str,
            timeout_current_s3_record: int = 0,
        ) -> None:
            timeout_sqs_continuing_queue = os.environ["SQS_CONTINUE_URL"]

            shared_logger.info(
                "lambda is going to shutdown, continuing on dedicated sqs queue",
                extra={
                    "sqs_queue": timeout_sqs_continuing_queue,
                    "sent_events": timeout_sent_events,
                    "empty_events": timeout_empty_events,
                    "skipped_events": timeout_skipped_events,
                },
            )

            lambda_event["Records"] = remaining_sqs_records
            for timeout_current_sqs_record, timeout_sqs_record in enumerate(lambda_event["Records"]):
                if timeout_current_sqs_record > 0:
                    timeout_last_ending_offset = None
                    timeout_current_s3_record = 0

                timeout_input_id = timeout_sqs_record["eventSourceARN"]
                if (
                    "messageAttributes" in timeout_sqs_record
                    and "originalEventSourceARN" in timeout_sqs_record["messageAttributes"]
                ):
                    timeout_input_id = timeout_sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

                timeout_input_type = config.get_input_type_by_id(input_id=timeout_input_id)

                if timeout_input_type == "s3-sqs":
                    _handle_s3_sqs_continuation(
                        sqs_client=sqs_client,
                        sqs_continuing_queue=timeout_sqs_continuing_queue,
                        last_ending_offset=timeout_last_ending_offset,
                        sqs_record=timeout_sqs_record,
                        current_s3_record=timeout_current_s3_record,
                        event_input_id=timeout_input_id,
                        config_yaml=timeout_config_yaml,
                    )
                elif timeout_input_type == "sqs" or timeout_input_type == "cloudwatch-logs":
                    _handle_sqs_continuation(
                        sqs_client=sqs_client,
                        sqs_continuing_queue=timeout_sqs_continuing_queue,
                        last_ending_offset=timeout_last_ending_offset,
                        sqs_record=timeout_sqs_record,
                        event_input_id=timeout_input_id,
                        config_yaml=timeout_config_yaml,
                    )

                delete_sqs_record(sqs_record["eventSourceARN"], sqs_record["receiptHandle"])

        previous_sqs_record = 0
        for current_sqs_record, sqs_record in enumerate(lambda_event["Records"]):
            if current_sqs_record > previous_sqs_record:
                deleting_sqs_record = lambda_event["Records"][previous_sqs_record]
                delete_sqs_record(deleting_sqs_record["eventSourceARN"], deleting_sqs_record["receiptHandle"])

                previous_sqs_record = current_sqs_record

            is_continuation_of_cloudwatch_logs = is_continuing_of_cloudwatch_logs(sqs_record)

            input_id = sqs_record["eventSourceARN"]
            if "messageAttributes" in sqs_record and "originalEventSourceARN" in sqs_record["messageAttributes"]:
                input_id = sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

            input_type: Optional[str] = ""
            if is_continuation_of_cloudwatch_logs:
                input_type = "cloudwatch-logs"
            else:
                input_type = config.get_input_type_by_id(input_id=input_id)

            assert input_type is not None

            event_input = config.get_input_by_type_and_id(input_type, input_id)
            if not event_input:
                shared_logger.warning("no input defined", extra={"input_type": input_type, "input_id": input_id})
                continue

            if input_id in composite_shipper_cache:
                composite_shipper = composite_shipper_cache[input_id]
            else:
                composite_shipper = get_shipper_from_input(
                    event_input=event_input,
                    lambda_event=lambda_event,
                    at_record=current_sqs_record,
                    config_yaml=config_yaml,
                )

                composite_shipper_cache[event_input.id] = composite_shipper

            if input_type == "sqs" or input_type == "cloudwatch-logs":
                for es_event, last_ending_offset in _handle_sqs_event(
                    sqs_record, is_continuation_of_cloudwatch_logs, input_id
                ):
                    timeout, sent_outcome = event_processing(
                        processing_composing_shipper=composite_shipper,
                        processing_es_event=es_event,
                    )

                    if sent_outcome == EVENT_IS_SENT:
                        sent_events += 1
                    elif sent_outcome == EVENT_IS_FILTERED:
                        skipped_events += 1
                    else:
                        empty_events += 1

                    if timeout:
                        for composite_shipper in composite_shipper_cache.values():
                            composite_shipper.flush()

                        handle_timeout(
                            remaining_sqs_records=lambda_event["Records"][current_sqs_record:],
                            timeout_last_ending_offset=last_ending_offset,
                            timeout_sent_events=sent_events,
                            timeout_empty_events=empty_events,
                            timeout_skipped_events=skipped_events,
                            timeout_config_yaml=config_yaml,
                        )

                        return "continuing"

            elif input_type == "s3-sqs":
                for es_event, last_ending_offset, current_s3_record in _handle_s3_sqs_event(sqs_record):
                    timeout, sent_outcome = event_processing(
                        processing_composing_shipper=composite_shipper,
                        processing_es_event=es_event,
                    )

                    if sent_outcome == EVENT_IS_SENT:
                        sent_events += 1
                    elif sent_outcome == EVENT_IS_FILTERED:
                        skipped_events += 1
                    else:
                        empty_events += 1

                    if timeout:
                        for composite_shipper in composite_shipper_cache.values():
                            composite_shipper.flush()

                        handle_timeout(
                            remaining_sqs_records=lambda_event["Records"][current_sqs_record:],
                            timeout_last_ending_offset=last_ending_offset,
                            timeout_sent_events=sent_events,
                            timeout_empty_events=empty_events,
                            timeout_skipped_events=skipped_events,
                            timeout_config_yaml=config_yaml,
                            timeout_current_s3_record=current_s3_record,
                        )

                        return "continuing"

        for composite_shipper in composite_shipper_cache.values():
            composite_shipper.flush()

        shared_logger.info(
            "lambda processed all the events",
            extra={"sent_events": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
        )

    return "completed"
