# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import re
from unittest import TestCase

import pytest

from share import Config, ElasticsearchOutput, Input, Output, parse_config


class DummyOutput(Output):
    def __init__(self, output_type: str):
        super(DummyOutput, self).__init__(output_type=output_type)


@pytest.mark.unit
class TestOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "Type must be one of elasticsearch"):
                DummyOutput(output_type="type")

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "Output type must be of type str"):
                DummyOutput(output_type=1)  # type:ignore

    def test_get_type(self) -> None:
        output = Output(output_type="elasticsearch")
        assert output.type == "elasticsearch"


@pytest.mark.unit
class TestElasticsearchOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid init with elasticsearch_url and http_auth"):
            elasticsearch = ElasticsearchOutput(
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.api_key
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("valid init with cloud_id and http_auth"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.api_key
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("valid init with elasticsearch_url and api key"):
            elasticsearch = ElasticsearchOutput(
                elasticsearch_url="elasticsearch_url",
                api_key="api_key",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("valid init with cloud_id and api key"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("neither elasticsearch_url or cloud_id"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output elasticsearch_url or cloud_id must be set"):
                ElasticsearchOutput(elasticsearch_url="", cloud_id="")

        with self.subTest("both elasticsearch_url and cloud_id"):
            elasticsearch = ElasticsearchOutput(
                elasticsearch_url="elasticsearch_url",
                cloud_id="cloud_id",
                api_key="api_key",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("no username or api_key"):
            with self.assertRaisesRegex(
                ValueError, "Elasticsearch Output username and password or api_key must be set"
            ):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("both username and api_key"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("with tags"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                dataset="dataset",
                tags=["tag1", "tag2", "tag3"],
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "default"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be set when using username"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="",
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("empty dataset"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                namespace="namespace",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == ""
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("empty namespace"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                dataset="dataset",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "default"
            assert elasticsearch.tags == []

        with self.subTest("empty tags"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                dataset="dataset",
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "default"
            assert elasticsearch.tags == []

        with self.subTest("elasticsearch_url not str"):
            with self.assertRaisesRegex(
                ValueError, re.escape("Elasticsearch Output elasticsearch_url must be of type str")
            ):
                ElasticsearchOutput(
                    elasticsearch_url=0,  # type:ignore
                    username="username",
                    password="password",
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output username must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="",
                    username=0,  # type:ignore
                    password="password",
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password=0,  # type:ignore
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("cloud_id not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output cloud_id must be of type str"):
                ElasticsearchOutput(
                    cloud_id=0,  # type:ignore
                    username="username",
                    password="password",
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("api_key not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output api_key must be of type str"):
                ElasticsearchOutput(
                    cloud_id="cloud_id",
                    api_key=0,  # type:ignore
                    dataset="dataset",
                    namespace="namespace",
                )

        with self.subTest("dataset not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output dataset must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    dataset=0,  # type:ignore
                    namespace="namespace",
                )

        with self.subTest("namespace not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output namespace must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    dataset="dataset",
                    namespace=0,  # type:ignore
                )


@pytest.mark.unit
class TestInput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid init"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

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

    def test_input_tags(self) -> None:
        with self.subTest("valid tags"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.tags = ["tag1", "tag2", "tag3"]

            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("tags not list"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                input_sqs.tags = "tag1"  # type:ignore

        with self.subTest("each tag not str"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Each tag must be of type str, given: \\['tag1', 2, 'tag3'\\]"):
                input_sqs.tags = ["tag1", 2, "tag3"]  # type:ignore

    def test_get_output_by_type(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.get_output_by_type(output_type="test") is None

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

    def test_add_output(self) -> None:
        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

        with self.subTest("wrong output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaises(AssertionError):
                input_sqs.add_output(output_type="wrong")

        with self.subTest("type is not str"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Output type must be of type str"):
                input_sqs.add_output(output_type=0)  # type:ignore

        with self.subTest("type is duplicated"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            with self.assertRaisesRegex(ValueError, "Duplicated Output elasticsearch"):
                input_sqs.add_output(
                    output_type="elasticsearch",
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    dataset="dataset",
                    namespace="namespace",
                )

    def test_get_output_types(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.get_output_types() == []

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert input_sqs.get_output_types() == ["elasticsearch"]

    def test_delete_output_by_type(self) -> None:
        with self.subTest("delete elasticsearch output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            input_sqs.delete_output_by_type("elasticsearch")
            assert input_sqs.get_output_types() == []

        with self.subTest("delete not existing output"):
            input_sqs = Input(input_type="sqs", input_id="id")
            with self.assertRaisesRegex(KeyError, "'type"):
                input_sqs.delete_output_by_type("type")


@pytest.mark.unit
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
            assert input_sqs.tags == []

    def test_add_input(self) -> None:
        with self.subTest("sqs input"):
            config = Config()
            config.add_input(Input(input_type="sqs", input_id="id"))
            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            config.add_input(Input(input_type="sqs", input_id="id2"))
            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id2")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id2"
            assert input_sqs.tags == []

        with self.subTest("duplicated sqs input"):
            config = Config()
            config.add_input(Input(input_type="sqs", input_id="id"))
            with self.assertRaisesRegex(ValueError, "duplicated input sqs/id"):
                config.add_input(Input(input_type="sqs", input_id="id"))


@pytest.mark.unit
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

        with self.subTest("tags not list"):
            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags: "tag1"
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
            """
                )

            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags: 1, 2, 3
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
            """
                )

        with self.subTest("each tag not str"):
            with self.assertRaisesRegex(
                ValueError, "Each tag must be of type str, given: \\[2021, {'key1': 'value1'}, 'tag3'\\]"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - 2021
                  - {"key1": "value1"}
                  - "tag3"
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
            """
                )

        with self.subTest("tags added at output level"):
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
                      tags:
                        - "tag1"
                        - "tag2"
                        - "tag3"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == []

        with self.subTest("tags added at input level and output level"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "input_tag1"
                  - "input_tag2"
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      dataset: "dataset"
                      namespace: "namespace"
                      tags:
                        - "tag1"
                        - "tag2"
                        - "tag3"
            """
            )

            input_sqs = config.get_input_by_type_and_id(input_type="sqs", input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["input_tag1", "input_tag2"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["input_tag1", "input_tag2"]

        with self.subTest("valid tags"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "tag1"
                  - "tag2"
                  - "tag3"
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
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "tag1"
                  - "tag2"
                  - "tag3"
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
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "tag1"
                  - "tag2"
                  - "tag3"
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
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("valid input valid elasticsearch output with cloud id and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "tag1"
                  - "tag2"
                  - "tag3"
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
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("valid input valid elasticsearch output cloud_id and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: sqs
                id: id
                tags:
                  - "tag1"
                  - "tag2"
                  - "tag3"
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
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.dataset == "dataset"
            assert elasticsearch.namespace == "namespace"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
