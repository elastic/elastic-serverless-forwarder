# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from copy import deepcopy
from typing import Any
from unittest import TestCase

import elasticsearch
import mock
import pytest
from elasticsearch import SerializationError

import share
from share.environment import get_environment
from share.version import version
from shippers import ElasticsearchShipper, JSONSerializer

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


class MockTransport(elasticsearch.Transport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.kwargs = kwargs


class MockClient(elasticsearch.Elasticsearch):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["transport_class"] = MockTransport
        super().__init__(*args, **kwargs)


_documents = []
_failures = []


def mock_bulk(
    client: elasticsearch.Elasticsearch, actions: list[dict[str, Any]], **kwargs: Any
) -> tuple[int, list[dict[str, Any]]]:
    global _documents
    _documents = [actions]
    assert client.transport.kwargs["headers"] == {
        "User-Agent": share.utils.create_user_agent(esf_version=version, environment=get_environment())
    }
    return len(actions), []


def mock_bulk_failure(
    client: elasticsearch.Elasticsearch, actions: list[dict[str, Any]], **kwargs: Any
) -> tuple[int, list[dict[str, Any]]]:
    global _failures
    _failures = list(map(lambda action: {"create": {"_id": action["_id"], "error": "an error"}}, actions))
    return len(actions), _failures


@pytest.mark.unit
class TestElasticsearchShipper(TestCase):
    @mock.patch("shippers.es.es_bulk", mock_bulk)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_send(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="",
            es_datastream_name="logs-data.set-namespace",
            tags=["tag1", "tag2", "tag3"],
            batch_max_actions=0,
        )
        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert _documents == [
            [
                {
                    "@timestamp": _now,
                    "_index": "logs-data.set-namespace",
                    "_op_type": "create",
                    "data_stream": {"dataset": "data.set", "namespace": "namespace", "type": "logs"},
                    "event": {"dataset": "data.set"},
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
                    "tags": [
                        "forwarded",
                        "data-set",
                        "tag1",
                        "tag2",
                        "tag3",
                    ],
                }
            ]
        ]
        assert shipper._bulk_actions == []

    @mock.patch("shippers.es.es_bulk", mock_bulk_failure)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_send_with_failure(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="",
            es_datastream_name="data.set",
            tags=["tag1", "tag2", "tag3"],
            batch_max_actions=0,
        )
        es_event = deepcopy(_dummy_event)

        def event_id_generator(event: dict[str, Any]) -> str:
            return "_id"

        shipper.set_event_id_generator(event_id_generator=event_id_generator)
        shipper.send(es_event)

        assert _failures == [{"create": {"_id": "_id", "error": "an error"}}]
        assert shipper._bulk_actions == []

    @mock.patch("shippers.es.es_bulk", mock_bulk)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_flush(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="",
            es_datastream_name="logs-data.set-namespace",
            tags=["tag1", "tag2", "tag3"],
            batch_max_actions=2,
        )
        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert shipper._bulk_actions == [
            {
                "@timestamp": _now,
                "_index": "logs-data.set-namespace",
                "_op_type": "create",
                "data_stream": {"dataset": "data.set", "namespace": "namespace", "type": "logs"},
                "event": {"dataset": "data.set"},
                "aws": {
                    "s3": {
                        "bucket": {"arn": "bucket_name", "name": "arn:aws:s3:::bucket_name"},
                        "object": {"key": "file.key"},
                    }
                },
                "cloud": {"provider": "aws", "region": "aws-region"},
                "log": {"file": {"path": "https://bucket_name.s3.aws-region.amazonaws.com/file.key"}, "offset": 10},
                "message": "A dummy message",
                "tags": [
                    "forwarded",
                    "data-set",
                    "tag1",
                    "tag2",
                    "tag3",
                ],
            }
        ]

        shipper.flush()

        assert shipper._bulk_actions == []

        shipper.flush()

    @mock.patch("shippers.es.es_bulk", mock_bulk)
    @mock.patch("shippers.es.Elasticsearch", new=MockClient)
    def test_send_with_dataset_discovery(self) -> None:
        with self.subTest("empty es_datastream_name"):
            shipper = ElasticsearchShipper(
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                tags=["tag1", "tag2", "tag3"],
                batch_max_actions=0,
            )
            es_event = deepcopy(_dummy_event)
            shipper.send(es_event)

            assert shipper._dataset == "generic"
            assert shipper._namespace == "default"
            assert shipper._es_index == "logs-generic-default"

            assert _documents[0] == [
                {
                    "@timestamp": _now,
                    "_index": "logs-generic-default",
                    "_op_type": "create",
                    "data_stream": {"dataset": "generic", "namespace": "default", "type": "logs"},
                    "event": {"dataset": "generic"},
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
                    "tags": [
                        "forwarded",
                        "generic",
                        "tag1",
                        "tag2",
                        "tag3",
                    ],
                }
            ]

            assert shipper._bulk_actions == []

        with self.subTest("es_datastream_name as `logs-unit-test"):
            shipper = ElasticsearchShipper(
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="logs-unit-test",
                tags=["tag1", "tag2", "tag3"],
                batch_max_actions=0,
            )
            es_event = deepcopy(_dummy_event)
            shipper.send(es_event)

            assert shipper._dataset == "unit"
            assert shipper._namespace == "test"
            assert shipper._es_index == "logs-unit-test"

            assert _documents[0] == [
                {
                    "@timestamp": _now,
                    "_index": "logs-unit-test",
                    "_op_type": "create",
                    "data_stream": {"dataset": "unit", "namespace": "test", "type": "logs"},
                    "event": {"dataset": "unit"},
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
                    "tags": [
                        "forwarded",
                        "unit",
                        "tag1",
                        "tag2",
                        "tag3",
                    ],
                }
            ]

            assert shipper._bulk_actions == []

        with self.subTest("es_datastream_name not matching logs datastream naming convention"):
            shipper = ElasticsearchShipper(
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                tags=["tag1", "tag2", "tag3"],
                batch_max_actions=0,
            )
            es_event = deepcopy(_dummy_event)
            shipper.send(es_event)

            assert shipper._dataset == ""
            assert shipper._namespace == ""
            assert shipper._es_index == "es_datastream_name"

            assert _documents[0] == [
                {
                    "@timestamp": _now,
                    "_index": "es_datastream_name",
                    "_op_type": "create",
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
                    "tags": [
                        "forwarded",
                        "tag1",
                        "tag2",
                        "tag3",
                    ],
                }
            ]

            assert shipper._bulk_actions == []


@pytest.mark.unit
class TestDiscoverDataset(TestCase):
    def test_no_datastream(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            es_datastream_name="logs-es-index-no-datastream",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert shipper._es_index == "logs-es-index-no-datastream"

    def test_custom_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            es_datastream_name="logs-dataset-namespace",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert shipper._dataset == "dataset"
        assert shipper._namespace == "namespace"
        assert shipper._es_index == "logs-dataset-namespace"

    def test_aws_cloudtrail_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.cloudtrail"
        shipper.send(es_event)

        assert shipper._dataset == "aws.cloudtrail"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.cloudtrail-default"

    def test_aws_cloudwatch_logs_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.cloudwatch_logs"
        shipper.send(es_event)

        assert shipper._dataset == "aws.cloudwatch_logs"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.cloudwatch_logs-default"

    def test_aws_elb_logs_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.elb_logs"
        shipper.send(es_event)

        assert shipper._dataset == "aws.elb_logs"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.elb_logs-default"

    def test_aws_network_firewall_logs_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.firewall_logs"
        shipper.send(es_event)

        assert shipper._dataset == "aws.firewall_logs"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.firewall_logs-default"

    def test_aws_waf_logs_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.waf"
        shipper.send(es_event)

        assert shipper._dataset == "aws.waf"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.waf-default"

    def test_aws_vpcflow_logs_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        es_event["meta"]["integration_scope"] = "aws.vpcflow"
        shipper.send(es_event)

        assert shipper._dataset == "aws.vpcflow"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-aws.vpcflow-default"

    def test_unknown_dataset(self) -> None:
        shipper = ElasticsearchShipper(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            tags=["tag1", "tag2", "tag3"],
        )

        es_event = deepcopy(_dummy_event)
        shipper.send(es_event)

        assert shipper._dataset == "generic"
        assert shipper._namespace == "default"
        assert shipper._es_index == "logs-generic-default"


@pytest.mark.unit
class TestJSONSerializer(TestCase):
    def test_loads(self) -> None:
        json_serializer = JSONSerializer()

        with self.subTest("loads raises"):
            with self.assertRaises(SerializationError):
                json_serializer.loads("[")

    def test_dumps(self) -> None:
        json_serializer = JSONSerializer()

        with self.subTest("dumps raises"):
            with self.assertRaises(SerializationError):
                json_serializer.dumps(set())

        with self.subTest("dumps bytes"):
            dumped = json_serializer.dumps(b"bytes")
            assert "bytes" == dumped

        with self.subTest("dumps str"):
            dumped = json_serializer.dumps("string")
            assert "string" == dumped

        with self.subTest("dumps dict"):
            dumped = json_serializer.dumps({"key": "value"})
            assert '{"key":"value"}' == dumped
