# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import re
from typing import Any
from unittest import TestCase
from unittest.mock import Mock, patch

import mock

from share import Config, ElasticSearchOutput, Input, Output, parse_config


class MockOutput:
    @staticmethod
    def kwargs(value: dict[str, Any]) -> None:
        pass


class DummyOutput(Output):
    @property
    def kwargs(self) -> dict[str, Any]:
        return super(DummyOutput, self).kwargs

    @kwargs.setter
    def kwargs(self, value: dict[str, Any]) -> None:
        super(DummyOutput, self).kwargs(value)  # type:ignore

    def __init__(self, output_type: str, kwargs: dict[str, Any]):
        super(DummyOutput, self).__init__(output_type=output_type, kwargs=kwargs)


class TestOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "Type must be one of elasticsearch"):
                DummyOutput(output_type="type", kwargs={})

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "Output type must be of type str"):
                DummyOutput(output_type=1, kwargs={})  # type:ignore

        with self.subTest("kwargs rises"):
            with self.assertRaises(NotImplementedError):
                DummyOutput(output_type="elasticsearch", kwargs={})

    @mock.patch("share.Output.kwargs", new=MockOutput.kwargs)
    def test_get_type(self) -> None:
        Output.__abstractmethods__ = set()  # type:ignore
        output = Output(output_type="elasticsearch", kwargs={})  # type:ignore
        assert output.type == "elasticsearch"

    def test_get_kwargs(self) -> None:
        with self.assertRaises(NotImplementedError):
            output = DummyOutput(output_type="elasticsearch", kwargs={})
            output.kwargs

    def test_set_kwargs(self) -> None:
        with self.assertRaises(NotImplementedError):
            Output.__abstractmethods__ = set()  # type:ignore
            output = Output(output_type="elasticsearch", kwargs={})  # type:ignore
            getter_mock = Mock(wraps=output.kwargs.getter)  # type:ignore
            mock_property = output.kwargs.getter(getter_mock)  # type:ignore
            with patch.object(output, "kwargs", mock_property):
                output.kwargs = {"test": "test"}  # type:ignore


class TestElasticSearchOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid init with elasticsearch_url and http_auth"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                    "ignored": "ignored",
                },
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.api_key
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.kwargs == {
                "elasticsearch_url": "elasticsearch_url",
                "username": "username",
                "password": "password",
                "dataset": "dataset",
                "namespace": "namespace",
            }

        with self.subTest("valid init with cloud_id and http_auth"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "cloud_id": "cloud_id",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                    "ignored": "ignored",
                },
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.api_key
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.kwargs == {
                "cloud_id": "cloud_id",
                "username": "username",
                "password": "password",
                "dataset": "dataset",
                "namespace": "namespace",
            }

        with self.subTest("valid init with elasticsearch_url and api key"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "api_key": "api_key",
                    "dataset": "dataset",
                    "namespace": "namespace",
                    "ignored": "ignored",
                },
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.kwargs == {
                "elasticsearch_url": "elasticsearch_url",
                "api_key": "api_key",
                "dataset": "dataset",
                "namespace": "namespace",
            }

        with self.subTest("valid init with cloud_id and api key"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "cloud_id": "cloud_id",
                    "api_key": "api_key",
                    "dataset": "dataset",
                    "namespace": "namespace",
                    "ignored": "ignored",
                },
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.kwargs == {
                "cloud_id": "cloud_id",
                "api_key": "api_key",
                "dataset": "dataset",
                "namespace": "namespace",
            }

        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "output_type for ElasticSearchOutput must be elasticsearch"):
                ElasticSearchOutput(output_type="type", kwargs={})

        with self.subTest("neither elasticsearch_url or cloud_id"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output elasticsearch_url or cloud_id must be set"):
                ElasticSearchOutput(output_type="elasticsearch", kwargs={})

        with self.subTest("both elasticsearch_url and cloud_id"):
            with self.assertRaisesRegex(
                ValueError, "Elasticsearch Output only one between elasticsearch_url or cloud_id must be set"
            ):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={"elasticsearch_url": "elasticsearch_url", "cloud_id": "cloud_id"},
                )

        with self.subTest("no username or api_key"):
            with self.assertRaisesRegex(
                ValueError, "Elasticsearch Output username and password or api_key must be set"
            ):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("both username and api_key"):
            with self.assertRaisesRegex(
                ValueError, "Elasticsearch Output only one between username and password or api_key must be set"
            ):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "username": "username",
                        "api_key": "api_key",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be set when using username"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "username": "username",
                        "password": "",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("empty dataset"):
            with self.assertRaisesRegex(ValueError, "Empty param dataset provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "username": "username",
                        "password": "password",
                        "dataset": "",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("empty namespace"):
            with self.assertRaisesRegex(ValueError, "Empty param namespace provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "",
                    },
                )

        with self.subTest("elasticsearch_url not str"):
            with self.assertRaisesRegex(
                ValueError, re.escape("Elasticsearch Output elasticsearch_url must be of type str")
            ):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": 0,
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output username must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "",
                        "username": 0,
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "",
                        "username": "username",
                        "password": 0,
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("cloud_id not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output cloud_id must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "cloud_id": 0,
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("api_key not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output api_key must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "cloud_id": "cloud_id",
                        "api_key": 0,
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("dataset not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output dataset must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "",
                        "username": "username",
                        "password": "password",
                        "dataset": 0,
                        "namespace": "namespace",
                    },
                )

        with self.subTest("namespace not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output namespace must be of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "elasticsearch_url": "",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": 0,
                    },
                )

    def test_get_kwargs(self) -> None:
        with self.subTest("valid init"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                    "ignored": "ignored",
                },
            )

            assert elasticsearch.kwargs == {
                "elasticsearch_url": "elasticsearch_url",
                "username": "username",
                "password": "password",
                "dataset": "dataset",
                "namespace": "namespace",
            }


class TestInput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid init"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "Input type must be one of sqs"):
                Input(input_type="type", input_id="id")

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "Input type must be of type str"):
                Input(input_type=0, input_id="id")  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "Input id must be of type str"):
                Input(input_type="sqs", input_id=0)  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "Input id must be of type str"):
                Input(input_type="sqs", input_id=0)  # type:ignore

    def test_get_output_by_type(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.get_output_by_type(output_type="test") is None

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticSearchOutput)

    def test_add_output(self) -> None:
        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticSearchOutput)

        with self.subTest("wrong output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaises(AssertionError):
                input_sqs.add_output(output_type="wrong", output_kwargs={})

        with self.subTest("type is not str"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Output type must be of type str"):
                input_sqs.add_output(output_type=0, output_kwargs={})  # type:ignore

        with self.subTest("kwargs is not dict"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, re.escape("Output arguments must be of type dict[str, Any]")):
                input_sqs.add_output(output_type="sqs", output_kwargs="")  # type:ignore

        with self.subTest("type is duplicated"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            with self.assertRaisesRegex(ValueError, "Duplicated Output elasticsearch"):
                input_sqs.add_output(
                    output_type="elasticsearch",
                    output_kwargs={
                        "elasticsearch_url": "elasticsearch_url",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

    def test_get_output_types(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.get_output_types() == []

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            assert input_sqs.get_output_types() == ["elasticsearch"]

    def test_delete_output_by_type(self) -> None:
        with self.subTest("delete elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "elasticsearch_url": "elasticsearch_url",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            input_sqs.delete_output_by_type("elasticsearch")
            assert input_sqs.get_output_types() == []

        with self.subTest("delete not existing output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(KeyError, "'type"):
                input_sqs.delete_output_by_type("type")


class TestConfig(TestCase):
    def test_get_input_by_type_and_id(self) -> None:
        with self.subTest("none input"):
            config = Config()
            assert config.get_input_by_type_and_id(input_type="sqs", input_id="id") is None

            config.add_input(Input(input_type="sqs", input_id="id"))
            assert config.get_input_by_type_and_id(input_type="sqs", input_id="another_id") is None

        with self.subTest("sqs input"):
            config = Config()
            config.add_input(Input(input_type="sqs", input_id="id"))
            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

    def test_add_input(self) -> None:
        with self.subTest("sqs input"):
            config = Config()
            config.add_input(Input(input_type="sqs", input_id="id"))
            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

            config.add_input(Input(input_type="sqs", input_id="id2"))
            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id2")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id2"

        with self.subTest("duplicated sqs input"):
            config = Config()
            config.add_input(Input(input_type="sqs", input_id="id"))
            with self.assertRaisesRegex(ValueError, "duplicated input sqs/id"):
                config.add_input(Input(input_type="sqs", input_id="id"))


class TestParseConfig(TestCase):
    def test_parse_config(self) -> None:
        with self.subTest("empty config"):
            with self.assertRaises(AssertionError):
                parse_config(config_yaml="")

            with self.subTest("no inputs"):
                with self.assertRaisesRegex(ValueError, "No inputs provided"):
                    parse_config(
                        config_yaml="""
        config:
        """
                    )

                with self.assertRaisesRegex(ValueError, "No inputs provided"):
                    parse_config(
                        config_yaml="""
        inputs: {}
                """
                    )

        with self.subTest("no input type"):
            with self.assertRaisesRegex(ValueError, "Must be provided str type for input"):
                parse_config(
                    config_yaml="""
        inputs:
          - id: id
        """
                )

            with self.assertRaisesRegex(ValueError, "Must be provided str type for input"):
                parse_config(
                    config_yaml="""
        inputs:
          - type: {}
        """
                )

        with self.subTest("no input id"):
            with self.assertRaisesRegex(ValueError, "Must be provided str id for input"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: type
            """
                )

        with self.assertRaisesRegex(ValueError, "Must be provided str id for input"):
            parse_config(
                config_yaml="""
        inputs:
          - type: type
            id: {}
        """
            )

        with self.subTest("no valid input type"):
            with self.assertRaisesRegex(ValueError, "Input type must be one of sqs"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: type
                id: id
            """
                )

        with self.subTest("no input output"):
            with self.assertRaisesRegex(ValueError, "No valid outputs for input"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
            """
                )

            with self.assertRaisesRegex(ValueError, "No valid outputs for input"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs: {}
            """
                )

        with self.subTest("no valid input output type"):
            with self.assertRaisesRegex(ValueError, "Must be provided str type for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - args: {}
            """
                )

            with self.assertRaisesRegex(ValueError, "Must be provided str type for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: {}
            """
                )

        with self.subTest("no valid input args type"):
            with self.assertRaisesRegex(ValueError, "Must be provided dict args for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: type
            """
                )

            with self.assertRaisesRegex(ValueError, "Must be provided dict args for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: type
                    args: args
            """
                )

        with self.subTest("not valid input output"):
            with self.assertRaises(AssertionError):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: type
                    args: {}
            """
                )

            with self.assertRaisesRegex(ValueError, "Elasticsearch Output elasticsearch_url or cloud_id must be set"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args: {}
            """
                )

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      elasticsearch_url: "elasticsearch_url"
                      username: "username"
                      password: "password"
                      dataset: "dataset"
                      namespace: "namespace"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticSearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      elasticsearch_url: "elasticsearch_url"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticSearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"

        with self.subTest("valid input valid elasticsearch output with cloud id and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      username: "username"
                      password: "password"
                      dataset: "dataset"
                      namespace: "namespace"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticSearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"

        with self.subTest("valid input valid elasticsearch output cloud_id and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticSearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
