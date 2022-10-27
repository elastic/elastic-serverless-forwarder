# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any
from unittest import TestCase, mock

import pytest

from shippers.logstash import _EVENT_SENT, LogstashShipper

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


class MockResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


_200_OK = mock.MagicMock(return_value=MockResponse(200))
_429_TOO_MANY_REQUESTS = mock.MagicMock(return_value=MockResponse(429))


@pytest.mark.unit
class TestLogstashShipper(TestCase):
    @mock.patch("shippers.logstash.requests.Session.put", _200_OK)
    def test_send_successful(self) -> None:
        logstash_shipper = LogstashShipper(logstash_url="http://logstash_url")
        assert logstash_shipper.send(_dummy_event) == _EVENT_SENT

    @mock.patch("shippers.logstash.requests.Session.put", _429_TOO_MANY_REQUESTS)
    def test_send_too_many_requests(self) -> None:
        logstash_shipper = LogstashShipper(logstash_url="http://logstash_url")
        with self.assertRaisesRegex(RuntimeError, "Errors while sending data to Logstash. Return code 429"):
            logstash_shipper.send(_dummy_event)

    @mock.patch("shippers.logstash.requests.Session.put", _200_OK)
    def test_flush(self) -> None:
        logstash_shipper = LogstashShipper(logstash_url="http://logstash_url", max_batch_size=2)
        logstash_shipper.send(_dummy_event)
        assert logstash_shipper._events_batch == [_dummy_event]
        logstash_shipper.flush()
        assert logstash_shipper._events_batch == []
