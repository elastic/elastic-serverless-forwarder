# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from typing import Any, Callable, Optional

from aws_lambda_typing import context as context_

from share import (
    ExpandEventListFromField,
    # events_forwarded_telemetry,
    # function_ended_telemetry,
    function_started_telemetry,
    # input_processed_telemetry,
    json_parser,
    parse_config,
    shared_logger,
)
from share.secretsmanager import aws_sm_expander
from shippers import EVENT_IS_FILTERED, EVENT_IS_SENT, CompositeShipper, ProtocolShipper

from .cloudwatch_logs_trigger import (
    _from_awslogs_data_to_event,
    _handle_cloudwatch_logs_continuation,
    _handle_cloudwatch_logs_event,
)
from .kinesis_trigger import _handle_kinesis_continuation, _handle_kinesis_record
from .replay_trigger import ReplayedEventReplayHandler, get_shipper_for_replay_event
from .s3_sqs_trigger import _handle_s3_sqs_continuation, _handle_s3_sqs_event
from .sqs_trigger import _handle_sqs_continuation, _handle_sqs_event
from .utils import (
    CONFIG_FROM_PAYLOAD,
    INTEGRATION_SCOPE_GENERIC,
    ConfigFileException,
    TriggerTypeException,
    anonymize_arn,
    build_function_context,
    capture_serverless,
    config_yaml_from_payload,
    config_yaml_from_s3,
    delete_sqs_record,
    expand_event_list_from_field_resolver,
    get_continuing_original_input_type,
    get_input_from_log_group_subscription_data,
    get_shipper_from_input,
    get_sqs_client,
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

    shared_logger.debug("lambda triggered", extra={"invoked_function_arn": lambda_context.invoked_function_arn})

    function_started_telemetry(ctx=build_function_context(lambda_context))

    try:
        trigger_type, config_source = get_trigger_type_and_config_source(lambda_event)
        shared_logger.info("trigger", extra={"type": trigger_type})
    except Exception as e:
        raise TriggerTypeException(e)

    try:
        if config_source == CONFIG_FROM_PAYLOAD:
            config_yaml = config_yaml_from_payload(lambda_event)
        else:
            config_yaml = config_yaml_from_s3()
    except Exception as e:
        raise ConfigFileException(e)

    if config_yaml == "":
        raise ConfigFileException("Empty config")

    try:
        config = parse_config(config_yaml, _expanders)
    except Exception as e:
        raise ConfigFileException(e)

    assert config is not None

    sqs_client = get_sqs_client()

    if trigger_type == "replay-sqs":
        shared_logger.info("trigger", extra={"size": len(lambda_event["Records"])})

        replay_queue_arn = lambda_event["Records"][0]["eventSourceARN"]
        replay_handler = ReplayedEventReplayHandler(replay_queue_arn=replay_queue_arn)
        shipper_cache: dict[str, ProtocolShipper] = {}
        for replay_record in lambda_event["Records"]:
            event = json_parser(replay_record["body"])
            input_id = event["event_input_id"]
            output_type = event["output_type"]
            shipper_id = input_id + output_type
            if shipper_id not in shipper_cache:
                shipper = get_shipper_for_replay_event(
                    config=config,
                    output_type=output_type,
                    output_args=event["output_args"],
                    event_input_id=input_id,
                    replay_handler=replay_handler,
                )

                if shipper is None:
                    shared_logger.warning(
                        "no shipper for output in replay queue",
                        extra={"output_type": event["output_type"], "event_input_id": event["event_input_id"]},
                    )
                    continue

                shipper_cache[shipper_id] = shipper
            else:
                shipper = shipper_cache[shipper_id]

            shipper.send(event["event_payload"])
            event_uniq_id: str = event["event_payload"]["_id"] + output_type
            replay_handler.add_event_with_receipt_handle(
                event_uniq_id=event_uniq_id, receipt_handle=replay_record["receiptHandle"]
            )

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                shared_logger.info("lambda is going to shutdown")
                break

        for shipper in shipper_cache.values():
            shipper.flush()

        replay_handler.flush()
        shared_logger.info("lambda replayed all the events")
        return "replayed"

    sent_events: int = 0
    empty_events: int = 0
    skipped_events: int = 0

    if trigger_type == "cloudwatch-logs":
        cloudwatch_logs_event = _from_awslogs_data_to_event(lambda_event["awslogs"]["data"])

        shared_logger.info("trigger", extra={"size": len(cloudwatch_logs_event["logEvents"])})

        input_id, event_input = get_input_from_log_group_subscription_data(
            config,
            cloudwatch_logs_event["owner"],
            cloudwatch_logs_event["logGroup"],
            cloudwatch_logs_event["logStream"],
        )

        if event_input is None:
            shared_logger.warning("no input defined", extra={"input_type": trigger_type, "input_id": input_id})

            return "completed"

        aws_region = input_id.split(":")[3]
        composite_shipper = get_shipper_from_input(event_input=event_input, config_yaml=config_yaml)

        event_list_from_field_expander = ExpandEventListFromField(
            event_input.expand_event_list_from_field,
            INTEGRATION_SCOPE_GENERIC,
            expand_event_list_from_field_resolver,
            event_input.root_fields_to_add_to_expanded_event,
        )

        for (
            es_event,
            last_ending_offset,
            last_event_expanded_offset,
            current_log_event_n,
        ) in _handle_cloudwatch_logs_event(
            cloudwatch_logs_event,
            aws_region,
            event_input.id,
            event_list_from_field_expander,
            event_input.json_content_type,
            event_input.get_multiline_processor(),
        ):
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

                # events_forwarded_telemetry(sent=sent_events, empty=empty_events, skipped=skipped_events)

                composite_shipper.flush()

                _handle_cloudwatch_logs_continuation(
                    sqs_client=sqs_client,
                    sqs_continuing_queue=sqs_continuing_queue,
                    last_ending_offset=last_ending_offset,
                    last_event_expanded_offset=last_event_expanded_offset,
                    cloudwatch_logs_event=cloudwatch_logs_event,
                    current_log_event=current_log_event_n,
                    event_input_id=input_id,
                    config_yaml=config_yaml,
                )

                # function_ended_telemetry(to_be_continued=True)

                return "continuing"

        composite_shipper.flush()
        shared_logger.info(
            "lambda processed all the events",
            extra={"sent_event": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
        )

    if trigger_type == "kinesis-data-stream":
        shared_logger.info("trigger", extra={"size": len(lambda_event["Records"])})

        input_id = lambda_event["Records"][0]["eventSourceARN"]
        event_input = config.get_input_by_id(input_id)
        if event_input is None:
            shared_logger.warning("no input defined", extra={"input_id": input_id})

            # function_ended_telemetry()

            return "completed"

        composite_shipper = get_shipper_from_input(event_input=event_input, config_yaml=config_yaml)

        event_list_from_field_expander = ExpandEventListFromField(
            event_input.expand_event_list_from_field,
            INTEGRATION_SCOPE_GENERIC,
            expand_event_list_from_field_resolver,
            event_input.root_fields_to_add_to_expanded_event,
        )

        for (
            es_event,
            last_ending_offset,
            last_event_expanded_offset,
            current_kinesis_record_n,
        ) in _handle_kinesis_record(
            lambda_event,
            event_input.id,
            event_list_from_field_expander,
            event_input.json_content_type,
            event_input.get_multiline_processor(),
        ):
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

                # events_forwarded_telemetry(sent=sent_events, empty=empty_events, skipped=skipped_events)

                composite_shipper.flush()

                remaining_kinesis_records = lambda_event["Records"][current_kinesis_record_n:]
                for current_kinesis_record, kinesis_record in enumerate(remaining_kinesis_records):
                    continuing_last_ending_offset: Optional[int] = last_ending_offset
                    continuing_last_event_expanded_offset: Optional[int] = last_event_expanded_offset
                    if current_kinesis_record > 0:
                        continuing_last_ending_offset = None
                        continuing_last_event_expanded_offset = None

                    _handle_kinesis_continuation(
                        sqs_client=sqs_client,
                        sqs_continuing_queue=sqs_continuing_queue,
                        last_ending_offset=continuing_last_ending_offset,
                        last_event_expanded_offset=continuing_last_event_expanded_offset,
                        kinesis_record=kinesis_record,
                        event_input_id=input_id,
                        config_yaml=config_yaml,
                    )

                # function_ended_telemetry(to_be_continued=True)

                return "continuing"

        composite_shipper.flush()
        shared_logger.info(
            "lambda processed all the events",
            extra={"sent_event": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
        )

    if trigger_type == "s3-sqs" or trigger_type == "sqs":
        shared_logger.info("trigger", extra={"size": len(lambda_event["Records"])})

        composite_shipper_cache: dict[str, CompositeShipper] = {}

        def event_processing(
            processing_composing_shipper: CompositeShipper, processing_es_event: dict[str, Any]
        ) -> tuple[bool, str]:
            processing_sent_outcome = processing_composing_shipper.send(processing_es_event)

            if lambda_context is not None and lambda_context.get_remaining_time_in_millis() < _completion_grace_period:
                return True, processing_sent_outcome

            return False, processing_sent_outcome

        def handle_timeout(
            remaining_sqs_records: list[dict[str, Any]],
            timeout_last_ending_offset: Optional[int],
            timeout_last_event_expanded_offset: Optional[int],
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

            # events_forwarded_telemetry(sent=sent_events, empty=empty_events, skipped=skipped_events)

            for timeout_current_sqs_record, timeout_sqs_record in enumerate(remaining_sqs_records):
                if timeout_current_sqs_record > 0:
                    timeout_last_ending_offset = None
                    timeout_last_event_expanded_offset = None
                    timeout_current_s3_record = 0

                timeout_input_id = timeout_sqs_record["eventSourceARN"]
                if (
                    "messageAttributes" in timeout_sqs_record
                    and "originalEventSourceARN" in timeout_sqs_record["messageAttributes"]
                ):
                    timeout_input_id = timeout_sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

                timeout_input = config.get_input_by_id(input_id=timeout_input_id)

                if timeout_input is None:
                    continue

                if timeout_input.type == "s3-sqs":
                    _handle_s3_sqs_continuation(
                        sqs_client=sqs_client,
                        sqs_continuing_queue=timeout_sqs_continuing_queue,
                        last_ending_offset=timeout_last_ending_offset,
                        last_event_expanded_offset=timeout_last_event_expanded_offset,
                        sqs_record=timeout_sqs_record,
                        current_s3_record=timeout_current_s3_record,
                        event_input_id=timeout_input_id,
                        config_yaml=timeout_config_yaml,
                    )
                else:
                    _handle_sqs_continuation(
                        sqs_client=sqs_client,
                        sqs_continuing_queue=timeout_sqs_continuing_queue,
                        last_ending_offset=timeout_last_ending_offset,
                        last_event_expanded_offset=timeout_last_event_expanded_offset,
                        sqs_record=timeout_sqs_record,
                        event_input_id=timeout_input_id,
                        config_yaml=timeout_config_yaml,
                    )

                delete_sqs_record(sqs_record["eventSourceARN"], sqs_record["receiptHandle"])

        previous_sqs_record: int = 0
        last_sqs_record: Optional[dict[str, Any]] = None
        for current_sqs_record, sqs_record in enumerate(lambda_event["Records"]):
            last_sqs_record = sqs_record
            if current_sqs_record > previous_sqs_record:
                deleting_sqs_record = lambda_event["Records"][previous_sqs_record]
                delete_sqs_record(deleting_sqs_record["eventSourceARN"], deleting_sqs_record["receiptHandle"])

                previous_sqs_record = current_sqs_record

            continuing_original_input_type = get_continuing_original_input_type(sqs_record)

            is_continuing = False
            input_id = sqs_record["eventSourceARN"]
            if "messageAttributes" in sqs_record and "originalEventSourceARN" in sqs_record["messageAttributes"]:
                is_continuing = True
                input_id = sqs_record["messageAttributes"]["originalEventSourceARN"]["stringValue"]

            event_input = config.get_input_by_id(input_id)
            if event_input is None:
                shared_logger.warning("no input defined", extra={"input_id": input_id})
                continue

            # anonymized_arn = anonymize_arn(event_input.id)
            # input_processed_telemetry(input_id=anonymized_arn.id, is_continuing=is_continuing)

            if input_id in composite_shipper_cache:
                composite_shipper = composite_shipper_cache[input_id]
            else:
                composite_shipper = get_shipper_from_input(event_input=event_input, config_yaml=config_yaml)

                composite_shipper_cache[event_input.id] = composite_shipper

            continuing_event_expanded_offset: Optional[int] = None
            if (
                "messageAttributes" in sqs_record
                and "originalLastEventExpandedOffset" in sqs_record["messageAttributes"]
            ):
                continuing_event_expanded_offset = int(
                    sqs_record["messageAttributes"]["originalLastEventExpandedOffset"]["stringValue"]
                )

            if (
                event_input.type == "kinesis-data-stream"
                or event_input.type == "sqs"
                or event_input.type == "cloudwatch-logs"
            ):
                event_list_from_field_expander = ExpandEventListFromField(
                    event_input.expand_event_list_from_field,
                    INTEGRATION_SCOPE_GENERIC,
                    expand_event_list_from_field_resolver,
                    event_input.root_fields_to_add_to_expanded_event,
                    continuing_event_expanded_offset,
                )

                for es_event, last_ending_offset, last_event_expanded_offset in _handle_sqs_event(
                    sqs_record,
                    input_id,
                    event_list_from_field_expander,
                    continuing_original_input_type,
                    event_input.json_content_type,
                    event_input.get_multiline_processor(),
                ):
                    timeout, sent_outcome = event_processing(
                        processing_composing_shipper=composite_shipper, processing_es_event=es_event
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
                            timeout_last_event_expanded_offset=last_event_expanded_offset,
                            timeout_sent_events=sent_events,
                            timeout_empty_events=empty_events,
                            timeout_skipped_events=skipped_events,
                            timeout_config_yaml=config_yaml,
                        )

                        return "continuing"

            elif event_input.type == "s3-sqs":
                sqs_record_body: dict[str, Any] = json_parser(sqs_record["body"])
                for es_event, last_ending_offset, last_event_expanded_offset, current_s3_record in _handle_s3_sqs_event(
                    sqs_record_body,
                    event_input.id,
                    event_input.expand_event_list_from_field,
                    event_input.root_fields_to_add_to_expanded_event,
                    event_input.json_content_type,
                    event_input.get_multiline_processor(),
                ):
                    timeout, sent_outcome = event_processing(
                        processing_composing_shipper=composite_shipper, processing_es_event=es_event
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
                            timeout_last_event_expanded_offset=last_event_expanded_offset,
                            timeout_sent_events=sent_events,
                            timeout_empty_events=empty_events,
                            timeout_skipped_events=skipped_events,
                            timeout_config_yaml=config_yaml,
                            timeout_current_s3_record=current_s3_record,
                        )

                        # function_ended_telemetry(to_be_continued=True)

                        return "continuing"

        for composite_shipper in composite_shipper_cache.values():
            composite_shipper.flush()

        shared_logger.info(
            "lambda processed all the events",
            extra={"sent_events": sent_events, "empty_events": empty_events, "skipped_events": skipped_events},
        )

        # events_forwarded_telemetry(sent=sent_events, empty=empty_events, skipped=skipped_events)

        assert last_sqs_record is not None
        delete_sqs_record(last_sqs_record["eventSourceARN"], last_sqs_record["receiptHandle"])

    # function_ended_telemetry()

    return "completed"
