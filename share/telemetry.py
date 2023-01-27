# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import hashlib
import json
import os
import time
from abc import ABCMeta
from enum import Enum
from queue import Empty, Queue
from threading import Thread
from typing import Any, Optional, Protocol, TypeVar

import urllib3
from aws_lambda_typing import context as context_

from share import shared_logger


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError("invalid truth value {!r}".format(val))


def is_telemetry_enabled() -> bool:
    try:
        return strtobool(os.environ["TELEMETRY_ENABLED"])
    except (KeyError, ValueError):  # TELEMETRY_ENABLED not in env dict
        return False


class WithExceptionTelemetryEnum(Enum):
    EXCEPTION_RAISED = "EXCEPTION_RAISED"
    EXCEPTION_IGNORED = "EXCEPTION_IGNORED"


class TelemetryData:
    function_id: str = ""
    function_version: str = ""
    execution_id: str = ""
    cloud_provider: str = ""
    cloud_region: str = ""
    memory_limit_in_mb: str = ""

    start_time: str = ""
    end_time: str = ""

    with_exception: Optional[WithExceptionTelemetryEnum] = None
    to_be_continued: bool = False

    input_outputs_type: dict[str, list[str]] = {}
    input_is_continuing: dict[str, bool] = {}
    events_forwarded: dict[str, int] = {"sent": 0, "empty": 0, "skipped": 0}

    output_sent_to_replay: dict[str, int] = {}

    def add_output_type_for_input(self, input_id: str, output_type: str) -> None:
        if input_id not in self.input_outputs_type:
            self.input_outputs_type[input_id] = list()

        self.input_outputs_type[input_id].append(output_type)

    def mark_input_is_continuing(self, input_id: str, is_continuing: bool) -> None:
        self.input_is_continuing[input_id] = is_continuing

    def increase_output_sent_to_replay(self, output_type: str, replayed_events: int) -> None:
        if output_type not in self.output_sent_to_replay:
            self.output_sent_to_replay[output_type] = 0

        self.output_sent_to_replay[output_type] += replayed_events

    def increase_events_forwarded(self, sent_events: int, empty_events: int, skipped_events: int) -> None:
        self.events_forwarded["sent"] += sent_events
        self.events_forwarded["empty"] += empty_events
        self.events_forwarded["skipped"] += skipped_events


class ProtocolTelemetryCommand(Protocol):
    """
    Protocol for Telemetry Command components
    """

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        pass  # pragma: no cover


TelemetryCommandType = TypeVar("TelemetryCommandType", bound=ProtocolTelemetryCommand)


telemetry_queue: Queue[ProtocolTelemetryCommand] = Queue()


class CommonTelemetryCommand(metaclass=ABCMeta):
    """
    Common class for Telemetry Command components
    arn:partition:service:region:account-id:resource-id
    arn:partition:service:region:account-id:resource-type/resource-id
    arn:partition:service:region:account-id:resource-type:resource-id
    """

    @staticmethod
    def _from_aws_arn_to_id(aws_arn: str, include_service: bool = True) -> tuple[str, str]:
        arn_components = aws_arn.split(":")

        service = arn_components[2]
        region = arn_components[3]

        input_id = hashlib.sha256(aws_arn.encode("utf-8")).hexdigest()[:10]
        account_id = hashlib.sha256(arn_components[4].encode("utf-8")).hexdigest()[:10]

        id_components: list[str] = [f"{account_id}"]
        if include_service:
            id_components.append(service)

        id_components.append(region)
        id_components.append(f"{input_id}")

        return ":".join(id_components), region

    def _from_input_arn_to_id(self, input_arn: str) -> tuple[str, str]:
        return self._from_aws_arn_to_id(aws_arn=input_arn)

    def _from_lambda_arn_to_id(self, lambda_arn: str) -> tuple[str, str]:
        return self._from_aws_arn_to_id(aws_arn=lambda_arn, include_service=False)


class EventsForwardedCommand(CommonTelemetryCommand):
    """
    InputEventsForwarded Command.
    This class implements concrete InputEventsForwarded Command
    """

    def __init__(self, sent_events: int, empty_events: int, skipped_events: int) -> None:
        self.sent_events = sent_events
        self.empty_events = empty_events
        self.skipped_events = skipped_events

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.increase_events_forwarded(
            sent_events=self.sent_events,
            empty_events=self.empty_events,
            skipped_events=self.skipped_events,
        )

        return telemetry_data


class InputHasOutputTypeCommand(CommonTelemetryCommand):
    """
    InputHasOutputTypeCommand Command.
    This class implements concrete InputHasOutputTypeCommand Command
    """

    def __init__(self, input_arn: str, output_type: str) -> None:
        self.input_id, _ = self._from_input_arn_to_id(input_arn)
        self.output_type = output_type

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.add_output_type_for_input(input_id=self.input_id, output_type=self.output_type)
        return telemetry_data


class InputProcessedCommand(CommonTelemetryCommand):
    """
    InputProcessed Command.
    This class implements concrete InputProcessed Command
    """

    def __init__(self, input_arn: str, is_continuing: bool) -> None:
        self.input_id, _ = self._from_input_arn_to_id(input_arn)
        self.is_continuing = is_continuing

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.mark_input_is_continuing(input_id=self.input_id, is_continuing=self.is_continuing)
        return telemetry_data


class LambdaEndedCommand(CommonTelemetryCommand):
    """
    LambdaEnded Command.
    This class implements concrete LambdaEnded Command
    """

    def __init__(self, with_exception: Optional[WithExceptionTelemetryEnum], to_be_continued: bool) -> None:
        self.with_exception = with_exception
        self.to_be_continued = to_be_continued

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.with_exception = self.with_exception
        telemetry_data.to_be_continued = self.to_be_continued
        telemetry_data.end_time = datetime.datetime.utcnow().strftime("%s.%f")
        return telemetry_data


class LambdaStartedCommand(CommonTelemetryCommand):
    """
    LambdaStarted Command.
    This class implements concrete LambdaStarted Command
    """

    def __init__(self, lambda_arn: str, execution_id: str, memory_limit_in_mb: str, function_version: str) -> None:
        self.function_id, self.cloud_region = self._from_lambda_arn_to_id(lambda_arn)
        self.execution_id = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:10]
        self.memory_limit_in_mb = memory_limit_in_mb
        self.function_id += f":{function_version}"
        self.function_version = function_version

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.function_id = self.function_id
        telemetry_data.function_version = self.function_version
        telemetry_data.cloud_region = self.cloud_region
        telemetry_data.execution_id = self.execution_id
        telemetry_data.memory_limit_in_mb = self.memory_limit_in_mb
        telemetry_data.start_time = datetime.datetime.utcnow().strftime("%s.%f")
        telemetry_data.end_time = None

        return telemetry_data


class OutputEventsSentToReplayCommand(CommonTelemetryCommand):
    """
    OutputEventsSentToReplay Command.
    This class implements concrete OutputEventsSentToReplay Command
    """

    def __init__(self, output_type: str, replayed_events: int) -> None:
        self.output_type = output_type
        self.replayed_events = replayed_events

    def execute(self, telemetry_data: TelemetryData) -> TelemetryData:
        telemetry_data.increase_output_sent_to_replay(
            output_type=self.output_type, replayed_events=self.replayed_events
        )

        return telemetry_data


class TelemetryThread(Thread):
    def __init__(self, queue: Queue[ProtocolTelemetryCommand]) -> None:
        Thread.__init__(self)
        self.queue = queue
        self.telemetry_endpoint = os.environ.get(
            "TELEMETRY_ENDPOINT",
            # "https://telemetry-staging.elastic.co/v3/send/esf",
            "https://7p6anzoo1g.parrotbay.io",
        )
        self.telemetry_data = TelemetryData()
        self.telemetry_client = urllib3.PoolManager(
            # @TODO: make connect/read timeouts customizable
            timeout=urllib3.Timeout(
                connect=2.0,
                read=3.0,
            ),
            retries=False,
        )

    def _send_telemetry(self) -> None:
        telemetry_data: dict[str, Any] = {
            "function_id": self.telemetry_data.function_id,
            "execution_id": self.telemetry_data.execution_id,
            "cloud_region": self.telemetry_data.cloud_region,
            "memory_limit_in_mb": self.telemetry_data.memory_limit_in_mb,
            "start_time": self.telemetry_data.start_time,
        }

        if self.telemetry_data.end_time:
            telemetry_data.update(
                {
                    "end_time": self.telemetry_data.end_time,
                    "with_exception": self.telemetry_data.with_exception,
                    "to_be_continued": self.telemetry_data.to_be_continued,
                    "input_outputs_type": self.telemetry_data.input_outputs_type,
                    "input_is_continuing": self.telemetry_data.input_is_continuing,
                    "events_forwarded": self.telemetry_data.events_forwarded,
                    "output_sent_to_replay": self.telemetry_data.output_sent_to_replay,
                }
            )

        # @TODO: send the data
        try:
            encoded_data = json.dumps(telemetry_data).encode('utf-8')

            r = self.telemetry_client.request(
                "POST",
                self.telemetry_endpoint,
                body=encoded_data,
                headers={
                    "X-Elastic-Cluster-ID": self.telemetry_data.function_id,
                    "X-Elastic-Stack-Version": self.telemetry_data.function_version,
                    'Content-Type': 'application/json',
                }
            )
            shared_logger.info(f"telemetry data sent (http status: {r.status})")

        except Exception as e:
            shared_logger.info(f"telemetry data not sent: {e}")

    def _execute_command(self, command: ProtocolTelemetryCommand) -> None:
        self.telemetry_data = command.execute(self.telemetry_data)
        if isinstance(command, LambdaStartedCommand) or isinstance(command, LambdaEndedCommand):
            self._send_telemetry()

    def run(self) -> None:
        while True:
            try:
                telemetry_command: ProtocolTelemetryCommand = self.queue.get(block=False)
                self._execute_command(telemetry_command)
            except Empty:
                time.sleep(1)
                continue


if is_telemetry_enabled():
    telemetry_thread = TelemetryThread(telemetry_queue)
    telemetry_thread.daemon = True
    telemetry_thread.start()


def events_forwarded_telemetry(sent_events: int, empty_events: int, skipped_events: int) -> None:
    if not is_telemetry_enabled():
        return

    telemetry_command = EventsForwardedCommand(
        sent_events=sent_events, empty_events=empty_events, skipped_events=skipped_events
    )
    telemetry_queue.put(telemetry_command)


def input_has_output_type_telemetry(input_arn: str, output_type: str) -> None:
    if not is_telemetry_enabled():
        return

    telemetry_command = InputHasOutputTypeCommand(input_arn=input_arn, output_type=output_type)
    telemetry_queue.put(telemetry_command)


def input_processed_telemetry(input_arn: str, is_continuing: bool = False) -> None:
    if not is_telemetry_enabled():
        return

    telemetry_command = InputProcessedCommand(input_arn=input_arn, is_continuing=is_continuing)
    telemetry_queue.put(telemetry_command)


def lambda_ended_telemetry(
    exception_ignored: bool = False, exception_raised: bool = False, to_be_continued: bool = False
) -> None:
    if not is_telemetry_enabled():
        return

    with_exception = None
    if exception_ignored:
        with_exception = WithExceptionTelemetryEnum.EXCEPTION_IGNORED
    elif exception_raised:
        with_exception = WithExceptionTelemetryEnum.EXCEPTION_RAISED

    telemetry_command = LambdaEndedCommand(with_exception=with_exception, to_be_continued=to_be_continued)
    telemetry_queue.put(telemetry_command)


def lambda_started_telemetry(lambda_context: context_.Context) -> None:
    if not is_telemetry_enabled():
        return

    telemetry_command = LambdaStartedCommand(
        lambda_arn=lambda_context.invoked_function_arn,
        execution_id=lambda_context.aws_request_id,
        memory_limit_in_mb=lambda_context.memory_limit_in_mb,
        function_version=lambda_context.function_version,
    )
    telemetry_queue.put(telemetry_command)


def output_events_sent_to_replay_telemetry(output_type: str, replayed_events: int) -> None:
    if not is_telemetry_enabled():
        return

    telemetry_command = OutputEventsSentToReplayCommand(output_type=output_type, replayed_events=replayed_events)
    telemetry_queue.put(telemetry_command)
