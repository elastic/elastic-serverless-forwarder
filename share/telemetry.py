# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import hashlib
import json
import os
import time
from abc import ABCMeta
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Queue
from threading import Thread
from typing import Any, List, Optional, Protocol, TypeVar, Union

import urllib3

from share import Input, shared_logger

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value {!r}".format(val))


def is_telemetry_enabled() -> bool:
    try:
        return strtobool(os.environ["TELEMETRY_ENABLED"])
    except (KeyError, ValueError):  # TELEMETRY_ENABLED not in env dict
        return False


# -------------------------------------------------------
# Models
# -------------------------------------------------------


class WithExceptionTelemetryEnum(Enum):
    """Enum for telemetry exception data."""

    EXCEPTION_RAISED = "EXCEPTION_RAISED"
    EXCEPTION_IGNORED = "EXCEPTION_IGNORED"


@dataclass
class FunctionContext:
    """The function execution context."""

    function_id: str
    function_version: str
    execution_id: str
    cloud_region: str
    cloud_provider: str
    memory_limit_in_mb: str


class TelemetryData:
    """Telemetry data class"""

    function_id: str = ""
    function_version: str = ""
    execution_id: str = ""
    cloud_provider: str = ""
    cloud_region: str = ""
    memory_limit_in_mb: str = ""

    inputs: List[dict[str, Union[str, List[str]]]] = []
    input: dict[str, Union[str, List[str]]] = {}

    start_time: str = ""
    end_time: str = ""

    with_exception: Optional[WithExceptionTelemetryEnum] = None
    to_be_continued: bool = False

    input_outputs_type: dict[str, dict[str, str]] = {}
    input_is_continuing: dict[str, bool] = {}
    events_forwarded: dict[str, int] = {"sent": 0, "empty": 0, "skipped": 0}

    output_sent_to_replay: dict[str, int] = {}

    def add_input(self, input_type: str, outputs: List[str]) -> None:
        """Add the input to the telemetry."""
        self.inputs.append({"type": input_type, "outputs": outputs})

    def set_input(self, input_type: str, outputs: List[str]) -> None:
        """Set the input to the telemetry."""
        self.input = {"type": input_type, "outputs": outputs}

    def set_output_type_for_input(self, input_id: str, input_type: str, output_type: str) -> None:
        """Add the output type for the input."""
        if input_id not in self.input_outputs_type:
            self.input_outputs_type[input_id] = {
                "type": input_type,
                "output": output_type,
            }
        # self.input_outputs_type[input_id].append(output_type)

    def mark_input_is_continuing(self, input_id: str, is_continuing: bool) -> None:
        """Mark the input as continuing or not."""
        self.input_is_continuing[input_id] = is_continuing

    def increase_output_sent_to_replay(self, output_type: str, replayed_events: int) -> None:
        """Increase the stats for the output sent to replay."""
        if output_type not in self.output_sent_to_replay:
            self.output_sent_to_replay[output_type] = 0

        self.output_sent_to_replay[output_type] += replayed_events

    def increase_events_forwarded(self, sent: int, empty: int, skipped: int) -> None:
        """Increase the stats for the events forwarded."""
        self.events_forwarded["sent"] += sent
        self.events_forwarded["empty"] += empty
        self.events_forwarded["skipped"] += skipped


class ProtocolTelemetryEvent(Protocol):
    """
    Protocol for Telemetry Command components
    """

    def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
        pass  # pragma: no cover


TelemetryEventType = TypeVar("TelemetryEventType", bound=ProtocolTelemetryEvent)


# -------------------------------------------------------
# Events
# -------------------------------------------------------

#
# Telemetry workflow
#
# - function started
# - config loaded
# - events forwarded, happens when the handler has sent events to the shipper
# - input has output, happens when an output is identified for the input
# - input processed, happens when the input is selected to process an incoming event
# - output sent to replay, IT LOOKS LIKE THIS IS NOT USED
# - function ended
#


class CommonTelemetryEvent(metaclass=ABCMeta):
    """
    Common class for Telemetry Command components
    arn:partition:service:region:account-id:resource-id
    arn:partition:service:region:account-id:resource-type/resource-id
    arn:partition:service:region:account-id:resource-type:resource-id
    """


class FunctionStartedEvent(CommonTelemetryEvent):
    """FunctionStartedEvent represents the start of the function execution."""

    def __init__(self, ctx: FunctionContext) -> None:
        self.function_id = ctx.function_id
        self.function_version = ctx.function_version
        self.cloud_provider = ctx.cloud_provider
        self.cloud_region = ctx.cloud_region
        self.execution_id = hashlib.sha256(ctx.execution_id.encode("utf-8")).hexdigest()[:10]
        self.memory_limit_in_mb = ctx.memory_limit_in_mb

    def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
        """Merge the current event details with the telemetry data"""
        telemetry_data.function_id = self.function_id
        telemetry_data.function_version = self.function_version
        telemetry_data.cloud_provider = self.cloud_provider
        telemetry_data.cloud_region = self.cloud_region
        telemetry_data.execution_id = self.execution_id
        telemetry_data.memory_limit_in_mb = self.memory_limit_in_mb
        telemetry_data.start_time = datetime.datetime.utcnow().strftime("%s.%f")
        telemetry_data.end_time = ""

        return telemetry_data


# class ConfigLoadedEvent(CommonTelemetryEvent):
#     """ConfigLoadedEvent represents the loading of the configuration."""

#     def __init__(self, config: Config) -> None:
#         self.config = config

#     def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
#         """Merge the current event details with the telemetry data"""

#         for _input in self.config.inputs.values():
#             telemetry_data.add_input(_input.type, _input.get_output_types())

#         return telemetry_data


# class FunctionEndedEvent(CommonTelemetryEvent):
#     """FunctionEndedEvent represents the end of the function execution."""
#
#     def __init__(self, with_exception: Optional[WithExceptionTelemetryEnum], to_be_continued: bool) -> None:
#         self.with_exception = with_exception
#         self.to_be_continued = to_be_continued
#
#     def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
#         """Merge the current event details with the telemetry data"""
#         telemetry_data.with_exception = self.with_exception
#         telemetry_data.to_be_continued = self.to_be_continued
#         telemetry_data.end_time = datetime.datetime.utcnow().strftime("%s.%f")
#         return telemetry_data


class InputSelectedEvent(CommonTelemetryEvent):
    """Happens when the input is selected to process an incoming event."""

    def __init__(self, input_type: str, outputs: List[str]) -> None:
        self.input_type = input_type
        self.outputs = outputs

    def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
        """Merge the current event details with the telemetry data"""
        telemetry_data.set_input(self.input_type, self.outputs)
        return telemetry_data


class InputProcessedEvent(CommonTelemetryEvent):
    """Happens when the input is selected to process an incoming event."""

    def __init__(self, input_id: str, is_continuing: bool) -> None:
        self.input_id = input_id
        self.is_continuing = is_continuing

    def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
        """Merge the current event details with the telemetry data"""
        telemetry_data.mark_input_is_continuing(input_id=self.input_id, is_continuing=self.is_continuing)
        return telemetry_data


# class OutputEventsSentToReplayEvent(CommonTelemetryEvent):
#     """
#     OutputEventsSentToReplay Command.
#     This class implements concrete OutputEventsSentToReplay Command
#     """

#     def __init__(self, output_type: str, replayed_events: int) -> None:
#         self.output_type = output_type
#         self.replayed_events = replayed_events

#     def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
#         """Merge the current event details with the telemetry data"""
#         telemetry_data.increase_output_sent_to_replay(
#             output_type=self.output_type, replayed_events=self.replayed_events
#         )

#         return telemetry_data


class InputHasOutputTypeEvent(CommonTelemetryEvent):
    """Happens when an output is identified for the input"""

    def __init__(self, input_id: str, input_type: str, output_type: str) -> None:
        self.input_id = input_id
        self.input_type = input_type
        self.output_type = output_type

    def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
        """Merge the current event details with the telemetry data"""
        telemetry_data.set_output_type_for_input(
            input_id=self.input_id,
            input_type=self.input_type,
            output_type=self.output_type,
        )

        return telemetry_data


# class EventsForwardedEvent(CommonTelemetryEvent):
#     """EventsForwardedEvent contains the stats about the events forwarded to the output."""
#
#     def __init__(self, sent: int, empty: int, skipped: int) -> None:
#         self.sent = sent
#         self.empty = empty
#         self.skipped = skipped
#
#     def merge_with(self, telemetry_data: TelemetryData) -> TelemetryData:
#         """Merge the current event details with the telemetry data"""
#         telemetry_data.increase_events_forwarded(
#             sent=self.sent,
#             empty=self.empty,
#             skipped=self.skipped,
#         )
#
#         return telemetry_data


# -------------------------------------------------------
# Worker Thread
# -------------------------------------------------------


class TelemetryWorker(Thread):
    """The TelemetryWorker sends the telemetry data to the telemetry endpoint.

    The worker waits for events to be added to the queue and then sends
    the telemetry data to the telemetry endpoint."""

    def __init__(self, queue: Queue[ProtocolTelemetryEvent]) -> None:
        Thread.__init__(self)
        self.queue = queue
        self.telemetry_data = TelemetryData()
        self.telemetry_endpoint = os.environ.get(
            "TELEMETRY_ENDPOINT",
            "https://telemetry-staging.elastic.co/v3/send/esf",  # fallback to staging
        )
        self.telemetry_client: urllib3.PoolManager = urllib3.PoolManager(
            # @TODO: make connect/read timeouts customizable
            timeout=urllib3.Timeout(
                connect=2.0,
                read=2.0,
            ),
            retries=False,  # we can't afford to retry on failure
        )

    def _send_telemetry(self) -> None:
        """Sends the telemetry data to the telemetry endpoint."""

        telemetry_data: dict[str, Any] = {
            "function_id": self.telemetry_data.function_id,
            "function_version": self.telemetry_data.function_version,
            "execution_id": self.telemetry_data.execution_id,
            "cloud_provider": self.telemetry_data.cloud_provider,
            "cloud_region": self.telemetry_data.cloud_region,
            "memory_limit_in_mb": self.telemetry_data.memory_limit_in_mb,
            # "start_time": self.telemetry_data.start_time,
        }

        if self.telemetry_data.input:
            telemetry_data.update(
                {
                    "input": self.telemetry_data.input,
                }
            )

        # if self.telemetry_data.end_time:
        #     telemetry_data.update(
        #         {
        #             "end_time": self.telemetry_data.end_time,
        #             "with_exception": self.telemetry_data.with_exception,
        #             "to_be_continued": self.telemetry_data.to_be_continued,
        #             # "input_outputs_type": self.telemetry_data.input_outputs_type,
        #             "input_is_continuing": self.telemetry_data.input_is_continuing,
        #             "events_forwarded": self.telemetry_data.events_forwarded,
        #             "output_sent_to_replay": self.telemetry_data.output_sent_to_replay,
        #         }
        #     )

        try:
            encoded_data = json.dumps(telemetry_data).encode("utf-8")
            r = self.telemetry_client.request(  # type: ignore
                "POST",
                self.telemetry_endpoint,
                body=encoded_data,
                headers={
                    "X-Elastic-Cluster-ID": self.telemetry_data.function_id,
                    "X-Elastic-Stack-Version": self.telemetry_data.function_version,
                    "Content-Type": "application/json",
                },
            )
            shared_logger.info(f"telemetry data sent (http status: {r.status})")

        except Exception as e:
            shared_logger.info(f"telemetry data not sent: {e}")

    def _process_event(self, event: ProtocolTelemetryEvent) -> None:
        """Process telemetry event"""

        self.telemetry_data = event.merge_with(self.telemetry_data)
        if isinstance(event, InputSelectedEvent):
            self._send_telemetry()

    def run(self) -> None:
        """The worker waits for events to be added to the queue and then sends
        the telemetry data to the telemetry endpoint."""

        while True:
            try:
                event: ProtocolTelemetryEvent = self.queue.get(block=False)
                self._process_event(event)
            except Empty:
                time.sleep(1)
                continue


# -------------------------------------------------------
# Module-scoped variables
# -------------------------------------------------------

telemetry_queue: Queue[ProtocolTelemetryEvent] = Queue()

if is_telemetry_enabled():
    telemetry_worker = TelemetryWorker(telemetry_queue)
    # the worker dies when main thread (only non-daemon thread) exits.
    telemetry_worker.daemon = True
    telemetry_worker.start()


# -------------------------------------------------------
# Event triggers
# -------------------------------------------------------


def function_started_telemetry(ctx: FunctionContext) -> None:
    """Triggers the `FunctionStartedEvent` telemetry event."""
    if not is_telemetry_enabled():
        return

    telemetry_queue.put(FunctionStartedEvent(ctx))


# def config_loaded_telemetry(config: Config) -> None:
#     """Triggers the `ConfigLoadedEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return

#     telemetry_queue.put(ConfigLoadedEvent(config))


# def function_ended_telemetry(
#     exception_ignored: bool = False, exception_raised: bool = False, to_be_continued: bool = False
# ) -> None:
#     """Triggers the `FunctionEndedEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return
#
#     with_exception = None
#     if exception_ignored:
#         with_exception = WithExceptionTelemetryEnum.EXCEPTION_IGNORED
#     elif exception_raised:
#         with_exception = WithExceptionTelemetryEnum.EXCEPTION_RAISED
#
#     telemetry_command = FunctionEndedEvent(with_exception=with_exception, to_be_continued=to_be_continued)
#     telemetry_queue.put(telemetry_command)


# def input_has_output_type_telemetry(input_id: str, input_type: str, output_type: str) -> None:
#     """Triggers the `InputHasOutputTypeEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return

#     telemetry_event = InputHasOutputTypeEvent(
#         input_id=input_id,
#         input_type=input_type,
#         output_type=output_type,
#     )

#     telemetry_queue.put(telemetry_event)


# def input_processed_telemetry(input_id: str, is_continuing: bool = False) -> None:
#     """Triggers the `InputProcessedEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return

#     telemetry_event = InputProcessedEvent(input_id=input_id, is_continuing=is_continuing)
#     telemetry_queue.put(telemetry_event)


def input_selected_telemetry(_input: Input) -> None:
    """Triggers the `InputSelectedEvent` telemetry event."""
    if not is_telemetry_enabled():
        return

    telemetry_event = InputSelectedEvent(
        _input.type,
        _input.get_output_types(),
    )
    telemetry_queue.put(telemetry_event)


# def output_events_sent_to_replay_telemetry(output_type: str, replayed_events: int) -> None:
#     """Triggers the `OutputEventsSentToReplayEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return

#     event = OutputEventsSentToReplayEvent(output_type=output_type, replayed_events=replayed_events)
#     telemetry_queue.put(event)


# def events_forwarded_telemetry(sent: int, empty: int, skipped: int) -> None:
#     """Triggers the `EventsForwardedEvent` telemetry event."""
#     if not is_telemetry_enabled():
#         return
#
#     event = EventsForwardedEvent(sent=sent, empty=empty, skipped=skipped)
#     telemetry_queue.put(event)
