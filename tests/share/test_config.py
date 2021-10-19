# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations
from typing import Any, Optional
from unittest import TestCase
from unittest.mock import patch, Mock

import mock
import re

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
            with self.assertRaisesRegex(ValueError, "Output type must by of type str"):
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
        with self.subTest("valid init"):
            elasticsearch = ElasticSearchOutput(
                output_type="elasticsearch",
                kwargs={
                    "hosts": ["hosts"],
                    "scheme": "scheme",
                    "username": "username",
                    "password": "password",
                    "dataset": "dataset",
                    "namespace": "namespace",
                },
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.hosts == ["hosts"]
            assert elasticsearch.scheme == "scheme"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.kwargs == {
                "hosts": ["hosts"],
                "scheme": "scheme",
                "username": "username",
                "password": "password",
                "dataset": "dataset",
                "namespace": "namespace",
            }

        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "output_type for ElasticSearchOutput must be elasticsearch"):
                ElasticSearchOutput(output_type="type", kwargs={})

        with self.subTest("missing kwargs"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "you must provide the following not empty init kwargs for elasticsearch: hosts, scheme, username,"
                    + " password, dataset, namespace. (provided: {})"
                ),
            ):
                ElasticSearchOutput(output_type="elasticsearch", kwargs={})

        with self.subTest("empty username"):
            with self.assertRaisesRegex(ValueError, "Empty param username provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": "",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "Empty param password provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
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
                        "hosts": ["hosts"],
                        "scheme": "scheme",
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
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "",
                    },
                )

        with self.subTest("empty scheme"):
            with self.assertRaisesRegex(ValueError, "Empty param scheme provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("empty host"):
            with self.assertRaisesRegex(ValueError, "Empty param hosts provided for Elasticsearch Output"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": [],
                        "scheme": "scheme",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("hosts not list"):
            with self.assertRaisesRegex(ValueError, re.escape("Elasticsearch Output hosts must by of type list[str]")):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": "",
                        "scheme": "scheme",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("scheme not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output scheme must by of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": 0,
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output username must by of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": 0,
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must by of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": "username",
                        "password": 0,
                        "dataset": "dataset",
                        "namespace": "namespace",
                    },
                )

        with self.subTest("dataset not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output dataset must by of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": "username",
                        "password": "password",
                        "dataset": 0,
                        "namespace": "namespace",
                    },
                )

        with self.subTest("namespace not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output namespace must by of type str"):
                ElasticSearchOutput(
                    output_type="elasticsearch",
                    kwargs={
                        "hosts": ["hosts"],
                        "scheme": "scheme",
                        "username": "username",
                        "password": "password",
                        "dataset": "dataset",
                        "namespace": 0,
                    },
                )


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
            with self.assertRaisesRegex(ValueError, "Input type must by of type str"):
                Input(input_type=0, input_id="id")  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "Input id must by of type str"):
                Input(input_type="sqs", input_id=0)  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "Input id must by of type str"):
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
                    "hosts": ["hosts"],
                    "scheme": "scheme",
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
                    "hosts": ["hosts"],
                    "scheme": "scheme",
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
            with self.assertRaisesRegex(ValueError, "Output type must by of type str"):
                input_sqs.add_output(output_type=0, output_kwargs={})  # type:ignore

        with self.subTest("kwargs is not dict"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, re.escape("Output arguments must by of type dict[str, Any]")):
                input_sqs.add_output(output_type="sqs", output_kwargs="")  # type:ignore

        with self.subTest("type is duplicated"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                output_kwargs={
                    "hosts": ["hosts"],
                    "scheme": "scheme",
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
                        "hosts": ["hosts"],
                        "scheme": "scheme",
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
                    "hosts": ["hosts"],
                    "scheme": "scheme",
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
                    "hosts": ["hosts"],
                    "scheme": "scheme",
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

            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "you must provide the following not empty init kwargs for elasticsearch: hosts, scheme, username,"
                    + " password, dataset, namespace. (provided: {})"
                ),
            ):
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

        with self.subTest("valid input valid elasticsearch output"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      hosts:
                        - "hosts"
                      scheme: "scheme"
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
            assert elasticsearch.hosts == ["hosts"]
            assert elasticsearch.scheme == "scheme"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
