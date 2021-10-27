# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any
from unittest import TestCase

import elasticsearch
import mock

from shippers import ElasticsearchShipper

_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
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
}


class MockTransport(elasticsearch.Transport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class MockClient(elasticsearch.Elasticsearch):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["transport_class"] = MockTransport
        super().__init__(*args, **kwargs)


_documents = []


def mock_bulk(client: Any, actions: list[dict[str, Any]]) -> None:
    global _documents
    _documents = [actions]


class TestElasticsearchShipper(TestCase):
    @mock.patch("shippers.es.es_bulk", mock_bulk)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_send(self) -> None:
        ElasticsearchShipper._bulk_batch_size = 0
        shipper = ElasticsearchShipper(
            hosts=["hosts"], username="username", password="", scheme="", dataset="data.set", namespace="namespace"
        )
        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert _documents == [
            [
                {
                    "@timestamp": _now,
                    "_id": "59273c1036-000000000010",
                    "_index": "logs-data.set-namespace",
                    "_op_type": "create",
                    "data_stream": {"dataset": "data.set", "namespace": "namespace", "type": "logs"},
                    "event": {"dataset": "data.set", "original": "A dummy message"},
                    "fields": {
                        "aws": {
                            "s3": {
                                "bucket": {"arn": "bucket_name", "name": "arn:aws:s3:::bucket_name"},
                                "object": {"key": "file.key"},
                            }
                        },
                        "cloud": {"provider": "aws", "region": "aws-region"},
                        "log": {
                            "file": {"path": "https://bucket_name.s3.aws-region.amazonaws.com/file.key"},
                            "offset": 10,
                        },
                        "message": "A dummy message",
                    },
                    "tags": ["preserve_original_event", "forwarded", "data-set"],
                }
            ]
        ]
        assert shipper._bulk_actions == []

    @mock.patch("shippers.es.es_bulk", mock_bulk)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_flush(self) -> None:
        ElasticsearchShipper._bulk_batch_size = 2
        shipper = ElasticsearchShipper(
            hosts=["hosts"], username="username", password="", scheme="", dataset="data.set", namespace="namespace"
        )
        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert shipper._bulk_actions == [
            {
                "@timestamp": _now,
                "_id": "59273c1036-000000000010",
                "_index": "logs-data.set-namespace",
                "_op_type": "create",
                "data_stream": {"dataset": "data.set", "namespace": "namespace", "type": "logs"},
                "event": {"dataset": "data.set", "original": "A dummy message"},
                "fields": {
                    "aws": {
                        "s3": {
                            "bucket": {"arn": "bucket_name", "name": "arn:aws:s3:::bucket_name"},
                            "object": {"key": "file.key"},
                        }
                    },
                    "cloud": {"provider": "aws", "region": "aws-region"},
                    "log": {"file": {"path": "https://bucket_name.s3.aws-region.amazonaws.com/file.key"}, "offset": 10},
                    "message": "A dummy message",
                },
                "tags": ["preserve_original_event", "forwarded", "data-set"],
            }
        ]

        shipper.flush()

        assert shipper._bulk_actions == []

        shipper.flush()
