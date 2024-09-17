# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import datetime
import gzip
from copy import deepcopy
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock

import pytest
import responses
import ujson
from requests import PreparedRequest

from shippers.logstash import _EVENT_SENT, _MAX_RETRIES, LogstashShipper

_now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

_dummy_event: dict[str, Any] = {
    "@timestamp": _now,
    "fields": {
        "message": "A dummy message",
        "log": {
            "offset": 10,
            "file": {
                "path": "https://bucket_name.s3.aws-region.amazonaws.com/file.key",
            },
        },
        "aws": {
            "s3": {
                "bucket": {
                    "name": "arn:aws:s3:::bucket_name",
                    "arn": "bucket_name",
                },
                "object": {
                    "key": "file.key",
                },
            },
        },
        "cloud": {
            "provider": "aws",
            "region": "aws-region",
        },
    },
    "meta": {},
}

_dummy_expected_event: dict[str, Any] = {
    "@timestamp": _now,
    "_id": "_id",
    "message": "A dummy message",
    "log": {
        "offset": 10,
        "file": {
            "path": "https://bucket_name.s3.aws-region.amazonaws.com/file.key",
        },
    },
    "aws": {
        "s3": {
            "bucket": {
                "name": "arn:aws:s3:::bucket_name",
                "arn": "bucket_name",
            },
            "object": {
                "key": "file.key",
            },
        },
    },
    "cloud": {
        "provider": "aws",
        "region": "aws-region",
    },
    "tags": ["forwarded"],
}


def _dummy_replay_handler(output_type: str, output_args: dict[str, Any], event_payload: dict[str, Any]) -> None:
    pass


@pytest.mark.unit
class TestLogstashShipper(TestCase):
    @responses.activate
    def test_send_successful(self) -> None:
        def request_callback(request: PreparedRequest) -> tuple[int, dict[Any, Any], str]:
            _payload = []
            assert request.headers["Content-Encoding"] == "gzip"
            assert request.headers["Content-Type"] == "application/x-ndjson"
            assert request.body is not None
            assert isinstance(request.body, bytes)

            events = gzip.decompress(request.body).decode("utf-8").split("\n")
            for event in events:
                _payload.append(ujson.loads(event))

            expected_event = deepcopy(_dummy_expected_event)
            expected_event["@metadata"] = {"_id": "_id"}
            del expected_event["_id"]

            assert _payload == [expected_event, expected_event]

            return 200, {}, "okay"

        def event_id_generator(event: dict[str, Any]) -> str:
            return "_id"

        url = "http://logstash_url"
        event = deepcopy(_dummy_event)
        responses.add_callback(responses.PUT, url, callback=request_callback)
        logstash_shipper = LogstashShipper(logstash_url=url, max_batch_size=2)
        logstash_shipper.set_event_id_generator(event_id_generator)
        logstash_shipper.send(event)
        logstash_shipper.send(event)

    @responses.activate
    def test_send_failures(self) -> None:
        url = "http://logstash_url"
        with self.subTest("Does not exceed max_retries"):
            responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            responses.put(url=url, status=200)
            event = deepcopy(_dummy_event)
            logstash_shipper = LogstashShipper(logstash_url=url)
            assert logstash_shipper.send(event) == _EVENT_SENT
        with self.subTest("Exceeds max retries, replay handler set"):
            for i in range(_MAX_RETRIES):
                responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            logstash_shipper = LogstashShipper(logstash_url=url)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper.set_replay_handler(replay_handler)
            event = deepcopy(_dummy_event)
            assert logstash_shipper.send(event) == _EVENT_SENT
            replay_handler.assert_called_once_with(url, {}, event)
        with self.subTest("Exceeds max retries, replay handler not set"):
            for i in range(_MAX_RETRIES):
                responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper = LogstashShipper(logstash_url=url)
            event = deepcopy(_dummy_event)
            assert logstash_shipper.send(event) == _EVENT_SENT
            replay_handler.assert_not_called()
        with self.subTest("Authentication error, request is not retried"):
            responses.put(url=url, status=401)
            logstash_shipper = LogstashShipper(logstash_url=url)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper.set_replay_handler(replay_handler)
            event = deepcopy(_dummy_event)
            assert logstash_shipper.send(event) == _EVENT_SENT
            replay_handler.assert_called_once_with(url, {}, event)

    @responses.activate
    def test_flush(self) -> None:
        url = "http://logstash_url"
        responses.put(url=url, status=200)
        responses.put(url=url, status=200)
        logstash_shipper = LogstashShipper(logstash_url=url, max_batch_size=2)
        event = deepcopy(_dummy_event)
        logstash_shipper.send(event)
        assert logstash_shipper._events_batch == [event]
        logstash_shipper.flush()
        assert logstash_shipper._events_batch == []
