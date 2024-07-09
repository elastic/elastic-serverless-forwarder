# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from typing import Any, Optional

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

from share import json_dumper, normalise_event, shared_logger
from shippers.shipper import EventIdGeneratorCallable, ReplayHandlerCallable

_EVENT_SENT = "_EVENT_SENT"
_EVENT_BUFFERED = "_EVENT_BUFFERED"

_TIMEOUT = 10
_MAX_RETRIES = 4
_STATUS_FORCE_LIST = [429, 500, 502, 503, 504]
# A backoff factor to apply between attempts after the second try. urllib3 will sleep for:
# {backoff factor} * (2 ** ({number of total retries} - 1))
# seconds. If the backoff_factor is 1, then sleep() will sleep for [0s, 2s, 4s, …] between retries.
_BACKOFF_FACTOR = 1


class LogstashAdapter(HTTPAdapter):
    """
    An HTTP adapter specific for Logstash that encapsulates the retry/backoff parameters and allows to verify
    certificates by SSL fingerprint
    """

    def __init__(self, fingerprint: str, *args, **kwargs):  # type: ignore
        self._fingerprint = fingerprint
        retry_strategy = Retry(total=_MAX_RETRIES, backoff_factor=_BACKOFF_FACTOR, status_forcelist=_STATUS_FORCE_LIST)
        HTTPAdapter.__init__(self, max_retries=retry_strategy, *args, **kwargs)  # type: ignore

    def init_poolmanager(self, *args, **kwargs):  # type: ignore
        if self._fingerprint:
            kwargs["assert_fingerprint"] = self._fingerprint
        return super().init_poolmanager(*args, **kwargs)  # type: ignore


class LogstashShipper:
    """
    Logstash Shipper.
    This class implements concrete Logstash Shipper
    """

    def __init__(
        self,
        logstash_url: str = "",
        username: str = "",
        password: str = "",
        max_batch_size: int = 1,
        compression_level: int = 9,
        ssl_assert_fingerprint: str = "",
        tags: list[str] = [],
    ) -> None:
        if logstash_url:
            self._logstash_url = logstash_url
        else:
            raise ValueError("You must provide logstash_url")

        self._replay_handler: Optional[ReplayHandlerCallable] = None
        self._event_id_generator: Optional[EventIdGeneratorCallable] = None
        self._events_batch: list[dict[str, Any]] = []

        self._max_batch_size = max_batch_size

        self._tags = tags

        if 0 <= compression_level <= 9:
            self._compression_level = compression_level
        else:
            raise ValueError("compression_level must be an integer value between 0 and 9")

        self._replay_args: dict[str, Any] = {}

        self._session = self._get_session(self._logstash_url, username, password, ssl_assert_fingerprint)

    @staticmethod
    def _get_session(url: str, username: str, password: str, ssl_assert_fingerprint: str) -> Session:
        session = Session()

        if username:
            session.auth = (username, password)

        if ssl_assert_fingerprint:
            session.verify = False

        session.mount(url, LogstashAdapter(ssl_assert_fingerprint))

        return session

    def send(self, event: dict[str, Any]) -> str:
        if "_id" not in event and self._event_id_generator is not None:
            event["_id"] = self._event_id_generator(event)

        event["tags"] = ["forwarded"]
        event["tags"] += self._tags

        event = normalise_event(event)

        # Let's move _id to @metadata._id for logstash
        if "_id" in event:
            event["@metadata"] = {"_id": event["_id"]}
            del event["_id"]

        self._events_batch.append(event)
        if len(self._events_batch) < self._max_batch_size:
            return _EVENT_BUFFERED

        self._send()

        return _EVENT_SENT

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        self._event_id_generator = event_id_generator

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        self._replay_handler = replay_handler

    def flush(self) -> None:
        if len(self._events_batch) > 0:
            self._send()

        self._events_batch = []

        return

    def _send(self) -> None:
        ndjson = "\n".join(json_dumper(event) for event in self._events_batch)

        try:
            response = self._session.put(
                self._logstash_url,
                data=gzip.compress(ndjson.encode("utf-8"), self._compression_level),
                headers={"Content-Encoding": "gzip", "Content-Type": "application/x-ndjson"},
                timeout=_TIMEOUT,
            )

            if response.status_code == 401:
                raise RequestException("Authentication error")
        except RequestException as e:
            shared_logger.error(
                f"logstash shipper encountered an error while publishing events to logstash. Error: {str(e)}"
            )

            if self._replay_handler is not None:
                for event in self._events_batch:
                    # let's put back the _id field from @metadata._id
                    if "@metadata" in event and "_id" in event["@metadata"]:
                        event["_id"] = event["@metadata"]["_id"]
                        del event["@metadata"]

                    self._replay_handler(self._logstash_url, self._replay_args, event)
