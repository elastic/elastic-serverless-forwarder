# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from typing import Any, Optional

import requests
import ujson

from shippers.shipper import EventIdGeneratorCallable, ReplayHandlerCallable

_EVENT_SENT = "_EVENT_SENT"
_EVENT_BUFFERED = "_EVENT_BUFFERED"


class LogstashShipper:
    """
    Logstash Shipper.
    This class implements concrete Logstash Shipper
    """

    def __init__(self, host: str = "", port: str = "", max_batch_size: int = 1) -> None:
        self._host = host
        self._port = port
        self._replay_handler: Optional[ReplayHandlerCallable] = None
        self._event_id_generator: Optional[EventIdGeneratorCallable] = None
        self._events_batch: list[dict[str, Any]] = []
        self._max_batch_size = max_batch_size

    def send(self, event: dict[str, Any]) -> str:
        self._events_batch.append(event)
        if len(self._events_batch) < self._max_batch_size:
            return _EVENT_BUFFERED
        self._send(self._host, self._port, self._events_batch)
        return _EVENT_SENT

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        self._event_id_generator = event_id_generator

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        self._replay_handler = replay_handler

    def flush(self) -> None:
        if len(self._events_batch) > 0:
            self._send(self._host, self._port, self._events_batch)
        self._events_batch = []
        return

    def _send(self, host: str, port: str, events: list[dict[str, Any]]) -> None:
        session = requests.Session()
        compression_level = 9
        ndjson = "\n".join(ujson.dumps(event) for event in events)
        response = session.put(
            f"http://{host}:{port}",
            data=gzip.compress(ndjson.encode("utf-8"), compression_level),
            headers={"Content-Encoding": "gzip", "Content-Type": "application/x-ndjson"},
        )
        if response.status_code != 200:
            # TODO: Change with actual handling after PoC
            raise RuntimeError(f"Errors while sending data to Logstash. Return code {response.status_code}")
