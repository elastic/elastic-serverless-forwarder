# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from typing import Any
from unittest import TestCase

import pytest

from share import Config, ElasticsearchOutput, IncludeExcludeFilter, IncludeExcludeRule, Input, Output, parse_config


class DummyOutput(Output):
    def __init__(self, output_type: str):
        super(DummyOutput, self).__init__(output_type=output_type)


@pytest.mark.unit
class TestOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("not valid type"):
            with self.assertRaisesRegex(ValueError, "^Type must be one of elasticsearch: another-type given$"):
                DummyOutput(output_type="another-type")

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
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.api_key
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("valid init with cloud_id and http_auth"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.api_key
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("valid init with elasticsearch_url and api key"):
            elasticsearch = ElasticsearchOutput(
                elasticsearch_url="elasticsearch_url",
                api_key="api_key",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("valid init with cloud_id and api key"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("neither elasticsearch_url or cloud_id"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output elasticsearch_url or cloud_id must be set"):
                ElasticsearchOutput(elasticsearch_url="", cloud_id="")

        with self.subTest("both elasticsearch_url and cloud_id"):
            elasticsearch = ElasticsearchOutput(
                elasticsearch_url="elasticsearch_url",
                cloud_id="cloud_id",
                api_key="api_key",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.cloud_id
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("no username or api_key"):
            with self.assertRaisesRegex(
                ValueError, "Elasticsearch Output username and password or api_key must be set"
            ):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("both username and api_key"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("with tags"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                tags=["tag1", "tag2", "tag3"],
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be set when using username"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("empty es_datastream_name"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == ""
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("empty tags"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("empty batch_max_actions"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_bytes=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 1

        with self.subTest("empty batch_max_bytes"):
            elasticsearch = ElasticsearchOutput(
                cloud_id="cloud_id",
                api_key="api_key",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
            )

            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert not elasticsearch.elasticsearch_url
            assert not elasticsearch.username
            assert not elasticsearch.password
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("elasticsearch_url not str"):
            with self.assertRaisesRegex(
                ValueError, re.escape("Elasticsearch Output elasticsearch_url must be of type str")
            ):
                ElasticsearchOutput(
                    elasticsearch_url=0,  # type:ignore
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output username must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="",
                    username=0,  # type:ignore
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output password must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password=0,  # type:ignore
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("cloud_id not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output cloud_id must be of type str"):
                ElasticsearchOutput(
                    cloud_id=0,  # type:ignore
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("api_key not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output api_key must be of type str"):
                ElasticsearchOutput(
                    cloud_id="cloud_id",
                    api_key=0,  # type:ignore
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("es_datastream_name not str"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output es_datastream_name must be of type str"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name=0,  # type:ignore
                )

        with self.subTest("batch_max_actions not int"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output batch_max_actions must be of type int"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                    batch_max_actions="test",  # type:ignore
                )

        with self.subTest("batch_max_bytes not int"):
            with self.assertRaisesRegex(ValueError, "Elasticsearch Output batch_max_bytes must be of type int"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                    batch_max_bytes="test",  # type:ignore
                )


@pytest.mark.unit
class TestInput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid s3-sqs init"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

        with self.subTest("valid sqs init"):
            input_sqs = Input(input_type="sqs", input_id="id")
            assert input_sqs.type == "sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

        with self.subTest("valid kinesis-data-stream init"):
            input_kinesis_data_stream = Input(input_type="kinesis-data-stream", input_id="id")
            assert input_kinesis_data_stream.type == "kinesis-data-stream"
            assert input_kinesis_data_stream.id == "id"
            assert input_kinesis_data_stream.tags == []

        with self.subTest("valid cloudwatch-logs init"):
            input_cloudwatch_logs = Input(input_type="cloudwatch-logs", input_id="id")
            assert input_cloudwatch_logs.type == "cloudwatch-logs"
            assert input_cloudwatch_logs.id == "id"
            assert input_cloudwatch_logs.tags == []

        with self.subTest("not valid type"):
            with self.assertRaisesRegex(
                ValueError,
                "^Input type must be one of cloudwatch-logs,s3-sqs,sqs,kinesis-data-stream: another-type given$",
            ):
                Input(input_type="another-type", input_id="id")

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "Input type must be of type str"):
                Input(input_type=0, input_id="id")  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "Input id must be of type str"):
                Input(input_type="s3-sqs", input_id=0)  # type:ignore

    def test_input_tags(self) -> None:
        with self.subTest("valid tags"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.tags = ["tag1", "tag2", "tag3"]

            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("tags not list"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                input_sqs.tags = "tag1"  # type:ignore

        with self.subTest("each tag not str"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Each tag must be of type str, given: \\['tag1', 2, 'tag3'\\]"):
                input_sqs.tags = ["tag1", 2, "tag3"]  # type:ignore

    def test_input_include_exclude_filter(self) -> None:
        with self.subTest("valid include_exclude_filter"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            include_exclude_filter = IncludeExcludeFilter()
            input_sqs.include_exclude_filter = include_exclude_filter

            assert input_sqs.include_exclude_filter == include_exclude_filter

        with self.subTest("include_exclude_filter not IncludeExcludeFilter"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "An error occurred while setting include and exclude filter"):
                input_sqs.include_exclude_filter = "wrong type"  # type:ignore

    def test_get_output_by_type(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            assert input_sqs.get_output_by_type(output_type="test") is None

        with self.subTest("elasticsearch output with legacy es_index_or_datastream_name"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_index_or_datastream_name="es_index_or_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

        with self.subTest("elasticsearch output with both legacy es_index_or_datastream_name and datastream"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_index_or_datastream_name="es_index_or_datastream_name",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

    def test_add_output(self) -> None:
        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert isinstance(input_sqs.get_output_by_type(output_type="elasticsearch"), ElasticsearchOutput)

        with self.subTest("not elasticsearch output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "^Type must be one of elasticsearch: another-type given$"):
                input_sqs.add_output(output_type="another-type")

        with self.subTest("type is not str"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "Output type must be of type str"):
                input_sqs.add_output(output_type=0)  # type:ignore

        with self.subTest("type is duplicated"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            with self.assertRaisesRegex(ValueError, "Duplicated Output elasticsearch"):
                input_sqs.add_output(
                    output_type="elasticsearch",
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                    batch_max_actions=1,
                    batch_max_bytes=1,
                )

    def test_get_output_types(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            assert input_sqs.get_output_types() == []

        with self.subTest("elasticsearch output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert input_sqs.get_output_types() == ["elasticsearch"]

    def test_delete_output_by_type(self) -> None:
        with self.subTest("delete elasticsearch output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            input_sqs.delete_output_by_type("elasticsearch")
            assert input_sqs.get_output_types() == []

        with self.subTest("delete not existing output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(KeyError, "'type"):
                input_sqs.delete_output_by_type("type")

    def test_discover_integration_scope(self) -> None:
        with self.subTest("integration_scope_discoverer is None"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            assert input_sqs.discover_integration_scope({}, 0) == ""

        with self.subTest("integration_scope_discoverer is not none"):

            def discover_integration_scope(lambda_event: dict[str, Any], at_record: int) -> str:
                return "an_integration_scope"

            input_sqs = Input(
                input_type="s3-sqs", input_id="id", integration_scope_discoverer=discover_integration_scope
            )

            assert input_sqs.discover_integration_scope({}, 0) == "an_integration_scope"


@pytest.mark.unit
class TestConfig(TestCase):
    def test_get_input_by_id(self) -> None:
        with self.subTest("none input"):
            config = Config()
            assert config.get_input_by_id(input_id="id") is None

            config.add_input(Input(input_type="s3-sqs", input_id="id"))
            assert config.get_input_by_id(input_id="another_id") is None

        with self.subTest("sqs input"):
            config = Config()
            config.add_input(Input(input_type="s3-sqs", input_id="id"))
            input_sqs = config.get_input_by_id(input_id="id")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

    def test_add_input(self) -> None:
        with self.subTest("sqs input"):
            config = Config()
            config.add_input(Input(input_type="s3-sqs", input_id="id"))
            input_sqs = config.get_input_by_id(input_id="id")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            config.add_input(Input(input_type="s3-sqs", input_id="id2"))
            input_sqs = config.get_input_by_id(input_id="id2")
            assert isinstance(input_sqs, Input)
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id2"
            assert input_sqs.tags == []

        with self.subTest("duplicated sqs input"):
            config = Config()
            config.add_input(Input(input_type="s3-sqs", input_id="id"))
            with self.assertRaisesRegex(ValueError, "duplicated input id"):
                config.add_input(Input(input_type="s3-sqs", input_id="id"))


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
            with self.assertRaisesRegex(
                ValueError,
                "^Input type must be one of cloudwatch-logs,s3-sqs,sqs,kinesis-data-stream: another-type given$",
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: another-type
                id: id
            """
                )

        with self.subTest("no input output"):
            with self.assertRaisesRegex(ValueError, "No valid outputs for input"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
            """
                )

            with self.assertRaisesRegex(ValueError, "No valid outputs for input"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs: {}
            """
                )

        with self.subTest("no valid input output type"):
            with self.assertRaisesRegex(ValueError, "Must be provided str type for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - args: {}
            """
                )

            with self.assertRaisesRegex(ValueError, "Must be provided str type for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
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
              - type: s3-sqs
                id: id
                outputs:
                  - type: type
            """
                )

            with self.assertRaisesRegex(ValueError, "Must be provided dict args for output"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: type
                    args: args
            """
                )

        with self.subTest("not valid input output"):
            with self.assertRaisesRegex(ValueError, "^Type must be one of elasticsearch: another-type given$"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: another-type
                    args:
                      key: value
            """
                )

            with self.assertRaisesRegex(ValueError, "Elasticsearch Output elasticsearch_url or cloud_id must be set"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args: {}
            """
                )

        with self.subTest("batch_max_actions not int"):
            with self.assertRaisesRegex(ValueError, "batch_max_actions must be of type int"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      batch_max_actions: "test"
            """
                )

        with self.subTest("batch_max_bytes not int"):
            with self.assertRaisesRegex(ValueError, "batch_max_bytes must be of type int"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      batch_max_bytes: "test"
            """
                )

        with self.subTest("tags not list"):
            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                tags: "tag1"
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

            with self.assertRaisesRegex(ValueError, "Tags must be of type list"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                tags: 1, 2, 3
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("each tag not str"):
            with self.assertRaisesRegex(
                ValueError, "Each tag must be of type str, given: \\[2021, {'key1': 'value1'}, 'tag3'\\]"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("valid input valid elasticsearch output with legacy es_index_or_datastream_name"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_index_or_datastream_name: "es_index_or_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.es_datastream_name == "es_index_or_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest(
            "valid input valid elasticsearch output with both legacy es_index_or_datastream_name and es_datastream_name"
        ):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
                      es_index_or_datastream_name: "es_index_or_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid input valid elasticsearch output with elasticsearch_url and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid input valid elasticsearch output with cloud id and http auth"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.username == "username"
            assert elasticsearch.password == "password"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid input valid elasticsearch output cloud_id and api key"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("tags added at output level"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      tags:
                        - "tag1"
                        - "tag2"
                        - "tag3"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("tags added at input level and output level"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                tags:
                  - "input_tag1"
                  - "input_tag2"
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      tags:
                        - "tag1"
                        - "tag2"
                        - "tag3"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["input_tag1", "input_tag2"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["input_tag1", "input_tag2"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid tags"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
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
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("valid include_exclude_filter"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                include:
                  - include_pattern1
                  - include_pattern2
                exclude:
                  - exclude_pattern1
                  - exclude_pattern2
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.include_exclude_filter == IncludeExcludeFilter(
                include_patterns=[
                    IncludeExcludeRule(pattern="include_pattern1"),
                    IncludeExcludeRule(pattern="include_pattern2"),
                ],
                exclude_patterns=[
                    IncludeExcludeRule(pattern="exclude_pattern1"),
                    IncludeExcludeRule(pattern="exclude_pattern2"),
                ],
            )

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("no list for include"):
            with self.assertRaisesRegex(ValueError, "Must be provided list type for include"):
                config = parse_config(
                    config_yaml="""
                inputs:
                  - type: s3-sqs
                    id: id
                    include:
                      include_pattern1
                    exclude:
                      - exclude_pattern1
                      - exclude_pattern2
                    outputs:
                      - type: elasticsearch
                        args:
                          cloud_id: "cloud_id"
                          api_key: "api_key"
                          es_datastream_name: "es_datastream_name"
                """
                )

        with self.subTest("no list for exclude"):
            with self.assertRaisesRegex(ValueError, "Must be provided list type for exclude"):
                config = parse_config(
                    config_yaml="""
                inputs:
                  - type: s3-sqs
                    id: id
                    include:
                      - include_pattern1
                      - include_pattern2
                    exclude:
                      exclude_pattern1
                    outputs:
                      - type: elasticsearch
                        args:
                          cloud_id: "cloud_id"
                          api_key: "api_key"
                          es_datastream_name: "es_datastream_name"
                """
                )

        with self.subTest("batch_max_actions not default"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      batch_max_actions: 1
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 10485760

        with self.subTest("batch_max_bytes not default"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
                      batch_max_bytes: 1
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            elasticsearch = input_sqs.get_output_by_type(output_type="elasticsearch")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 1
