# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import base64
import datetime
import gzip
import http
import http.server
import os
import ssl
import threading
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock
from base64 import b64encode

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

_username = "admin"
_password = "password"


def _dummy_replay_handler(output_type: str, output_args: dict[str, Any], event_payload: dict[str, Any]) -> None:
    assert output_type == "logstash"
    assert event_payload == _dummy_event


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

            expected_event = _dummy_event
            expected_event["_id"] = "_id"
            assert _payload == [expected_event, expected_event]

            return 200, {}, "okay"

        def event_id_generator(event: dict[str, Any]) -> str:
            return "_id"

        url = "http://logstash_url"
        responses.add_callback(responses.PUT, url, callback=request_callback)
        logstash_shipper = LogstashShipper(logstash_url=url, max_batch_size=2)
        logstash_shipper.set_event_id_generator(event_id_generator)
        logstash_shipper.send(_dummy_event)
        logstash_shipper.send(_dummy_event)

    @responses.activate
    def test_send_failures(self) -> None:
        url = "http://logstash_url"
        with self.subTest("Does not exceed max_retries"):
            responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            responses.put(url=url, status=200)
            logstash_shipper = LogstashShipper(logstash_url=url)
            assert logstash_shipper.send(_dummy_event) == _EVENT_SENT
        with self.subTest("Exceeds max retries, replay handler set"):
            for i in range(_MAX_RETRIES):
                responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            logstash_shipper = LogstashShipper(logstash_url=url)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper.set_replay_handler(replay_handler)
            assert logstash_shipper.send(_dummy_event) == _EVENT_SENT
            replay_handler.assert_called_once_with("logstash", {}, _dummy_event)
        with self.subTest("Exceeds max retries, replay handler not set"):
            for i in range(_MAX_RETRIES):
                responses.put(url=url, status=429)
            responses.put(url=url, status=429)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper = LogstashShipper(logstash_url=url)
            assert logstash_shipper.send(_dummy_event) == _EVENT_SENT
            replay_handler.assert_not_called()
        with self.subTest("Authentication error, request is not retried"):
            responses.put(url=url, status=401)
            logstash_shipper = LogstashShipper(logstash_url=url)
            replay_handler = MagicMock(side_effect=_dummy_replay_handler)
            logstash_shipper.set_replay_handler(replay_handler)
            assert logstash_shipper.send(_dummy_event) == _EVENT_SENT
            replay_handler.assert_called_once_with("logstash", {}, _dummy_event)

    def test_send_https_ssl_fingerprint(self) -> None:
        certpath = os.path.join(os.path.dirname(__file__), "ssl", "localhost.crt")
        keypath = os.path.join(os.path.dirname(__file__), "ssl", "localhost.pkcs8.key")
        server_address = ("localhost", 8080)
        httpd = http.server.HTTPServer(server_address, http.server.SimpleHTTPRequestHandler)
        httpd.socket = ssl.wrap_socket(
            httpd.socket, server_side=True, certfile=certpath, keyfile=keypath, ssl_version=ssl.PROTOCOL_TLS
        )
        httpd_thread = threading.Thread(target=httpd.serve_forever)
        httpd_thread.daemon = True
        httpd_thread.start()
        logstash_shipper = LogstashShipper(
            logstash_url="https://localhost:8080",
            ssl_assert_fingerprint="22:F7:FB:84" ":1D:43:3E" ":E7:BB:F9" ":72:F3:D8:97:AD:7C:86:E3:07:42",
        )
        replay_handler = MagicMock(side_effect=_dummy_replay_handler)
        logstash_shipper.set_replay_handler(replay_handler)
        assert logstash_shipper.send(_dummy_event) == _EVENT_SENT
        replay_handler.assert_not_called()
        httpd.shutdown()

    @responses.activate
    def test_send_basic_auth(self) -> None:
        def request_callback(request: PreparedRequest) -> tuple[int, dict[Any, Any], str]:
            _payload = []
            usr_bytes = _username.encode("latin1")
            pwd_bytes = _password.encode("latin1")
            auth_string = b64encode(b":".join((usr_bytes, pwd_bytes))).decode("latin1")
            assert request.headers["Content-Encoding"] == "gzip"
            assert request.headers["Content-Type"] == "application/x-ndjson"
            print(auth_string)
            assert request.headers["Authorization"] == f"Basic {auth_string}"
            assert request.body is not None
            assert isinstance(request.body, bytes)

            events = gzip.decompress(request.body).decode("utf-8").split("\n")
            for event in events:
                _payload.append(ujson.loads(event))

            expected_event = _dummy_event
            expected_event["_id"] = "_id"
            assert _payload == [expected_event, expected_event]

            return 200, {}, "okay"

        def event_id_generator(event: dict[str, Any]) -> str:
            return "_id"

        url = "http://logstash_url"
        responses.add_callback(responses.PUT, url, callback=request_callback)
        logstash_shipper = LogstashShipper(logstash_url=url, max_batch_size=2, username=_username, password=_password)
        logstash_shipper.set_event_id_generator(event_id_generator)
        logstash_shipper.send(_dummy_event)
        logstash_shipper.send(_dummy_event)

    @responses.activate
    def test_flush(self) -> None:
        url = "http://logstash_url"
        responses.put(url=url, status=200)
        responses.put(url=url, status=200)
        logstash_shipper = LogstashShipper(logstash_url=url, max_batch_size=2)
        logstash_shipper.send(_dummy_event)
        assert logstash_shipper._events_batch == [_dummy_event]
        logstash_shipper.flush()
        assert logstash_shipper._events_batch == []
