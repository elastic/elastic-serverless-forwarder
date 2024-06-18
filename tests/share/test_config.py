# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

import pytest

from share import (
    Config,
    CountMultiline,
    ElasticsearchOutput,
    IncludeExcludeFilter,
    IncludeExcludeRule,
    Input,
    LogstashOutput,
    Output,
    PatternMultiline,
    WhileMultiline,
    parse_config,
)


class DummyOutput(Output):
    def __init__(self, output_type: str):
        super(DummyOutput, self).__init__(output_type=output_type)


@pytest.mark.unit
class TestOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("not valid type"):
            with self.assertRaisesRegex(
                ValueError, "^`type` must be one of elasticsearch,logstash: another-type given$"
            ):
                DummyOutput(output_type="another-type")

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "`type` must be provided as string"):
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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("no username or api_key"):
            with self.assertRaisesRegex(ValueError, "One between `username` and `password`, or `api_key` must be set"):
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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "`password` must be set when using `username`"):
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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("empty ssl_assert_fingerprint"):
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
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("elasticsearch_url not str"):
            with self.assertRaisesRegex(ValueError, re.escape("`elasticsearch_url` must be provided as string")):
                ElasticsearchOutput(
                    elasticsearch_url=0,  # type:ignore
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "`username` must be provided as string"):
                ElasticsearchOutput(
                    elasticsearch_url="",
                    username=0,  # type:ignore
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "`password` must be provided as string"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password=0,  # type:ignore
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("cloud_id not str"):
            with self.assertRaisesRegex(ValueError, "`cloud_id` must be provided as string"):
                ElasticsearchOutput(
                    cloud_id=0,  # type:ignore
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("api_key not str"):
            with self.assertRaisesRegex(ValueError, "`api_key` must be provided as string"):
                ElasticsearchOutput(
                    cloud_id="cloud_id",
                    api_key=0,  # type:ignore
                    es_datastream_name="es_datastream_name",
                )

        with self.subTest("es_datastream_name not str"):
            with self.assertRaisesRegex(ValueError, "`es_datastream_name` must be provided as string"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name=0,  # type:ignore
                )

        with self.subTest("batch_max_actions not int"):
            with self.assertRaisesRegex(ValueError, "`batch_max_actions` must be provided as integer"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                    batch_max_actions="test",  # type:ignore
                )

        with self.subTest("batch_max_bytes not int"):
            with self.assertRaisesRegex(ValueError, "`batch_max_bytes` must be provided as integer"):
                ElasticsearchOutput(
                    elasticsearch_url="elasticsearch_url",
                    username="username",
                    password="password",
                    es_datastream_name="es_datastream_name",
                    batch_max_bytes="test",  # type:ignore
                )

            with self.subTest("ssl_assert_fingerprint not str"):
                with self.assertRaisesRegex(ValueError, "`ssl_assert_fingerprint` must be provided as string"):
                    ElasticsearchOutput(
                        elasticsearch_url="elasticsearch_url",
                        username="username",
                        password="password",
                        es_datastream_name="es_datastream_name",
                        ssl_assert_fingerprint=0,  # type:ignore
                    )


@pytest.mark.unit
class TestLogstashOutput(TestCase):
    def test_init(self) -> None:
        with self.subTest("valid init with valid url"):
            logstash = LogstashOutput(
                logstash_url="http://localhost:8080",
                tags=["tag1"],
            )

            assert logstash.type == "logstash"
            assert logstash.logstash_url == "http://localhost:8080"
        with self.subTest("valid init with valid url, max_batch_size and compression_level"):
            logstash = LogstashOutput(
                logstash_url="http://localhost:8080",
                max_batch_size=400,
                compression_level=2,
                tags=["tag1"],
            )

            assert logstash.type == "logstash"
            assert logstash.logstash_url == "http://localhost:8080"
            assert logstash.max_batch_size == 400
            assert logstash.compression_level == 2

        with self.subTest("logstash_url not string"):
            with self.assertRaisesRegex(ValueError, "`logstash_url` must be provided as string"):
                LogstashOutput(logstash_url=0)  # type: ignore

        with self.subTest("valid init with valid url, max_batch_size must be provided as int"):
            with self.assertRaisesRegex(ValueError, "`max_batch_size` must be provided as int"):
                LogstashOutput(
                    logstash_url="http://localhost:8080",
                    max_batch_size="string",  # type: ignore
                )

        with self.subTest("valid init with valid url, compression_level must be provided as int"):
            with self.assertRaisesRegex(ValueError, "`compression_level` must be provided as int"):
                LogstashOutput(
                    logstash_url="http://localhost:8080",
                    compression_level="string",  # type: ignore
                )

        with self.subTest("empty password"):
            with self.assertRaisesRegex(ValueError, "`password` must be set when using `username`"):
                LogstashOutput(logstash_url="http://localhost:8080", username="username")

        with self.subTest("username not str"):
            with self.assertRaisesRegex(ValueError, "`username` must be provided as string"):
                LogstashOutput(
                    logstash_url="http://localhost:8080",
                    username=0,  # type:ignore
                    password="password",
                )

        with self.subTest("password not str"):
            with self.assertRaisesRegex(ValueError, "`password` must be provided as string"):
                LogstashOutput(
                    logstash_url="http://localhost:8080",
                    username="username",
                    password=0,  # type:ignore
                )
        with self.subTest("ssl_assert_fingerprint not str"):
            with self.assertRaisesRegex(ValueError, "`ssl_assert_fingerprint` must be provided as string"):
                LogstashOutput(
                    logstash_url="http://localhost:8080",
                    ssl_assert_fingerprint=0,  # type:ignore
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
                "^`type` must be one of cloudwatch-logs,s3-sqs,sqs,kinesis-data-stream: another-type given$",
            ):
                Input(input_type="another-type", input_id="id")

        with self.subTest("type not str"):
            with self.assertRaisesRegex(ValueError, "`type` must be provided as string"):
                Input(input_type=0, input_id="id")  # type:ignore

        with self.subTest("id not str"):
            with self.assertRaisesRegex(ValueError, "`id` must be provided as string"):
                Input(input_type="s3-sqs", input_id=0)  # type:ignore

    def test_input_tags(self) -> None:
        with self.subTest("valid tags"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.tags = ["tag1", "tag2", "tag3"]

            assert input_sqs.tags == ["tag1", "tag2", "tag3"]

        with self.subTest("tags not list"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "`tags` must be provided as list for input id"):
                input_sqs.tags = "tag1"  # type:ignore

        with self.subTest("each tag not str"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(
                ValueError, "ach tag in `tags` must be provided as string for input id, given: \\['tag1', 2, 'tag3'\\]"
            ):
                input_sqs.tags = ["tag1", 2, "tag3"]  # type:ignore

    def test_input_expand_event_list_from_field(self) -> None:
        with self.subTest("expand_event_list_from_field not str"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(
                ValueError, "`expand_event_list_from_field` must be provided as string for input id"
            ):
                input_sqs.expand_event_list_from_field = 0  # type:ignore

    def test_input_json_content_type(self) -> None:
        with self.subTest("json_content_type not valid"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(
                ValueError, "`json_content_type` must be one of ndjson,single,disabled for input id: whatever given"
            ):
                input_sqs.json_content_type = "whatever"

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
            assert input_sqs.get_output_by_destination(output_destination="test") is None

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

            assert isinstance(
                input_sqs.get_output_by_destination(output_destination="elasticsearch_url"), ElasticsearchOutput
            )

        with self.subTest("logstash output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="logstash",
                logstash_url="logstash_url",
            )

            assert isinstance(input_sqs.get_output_by_destination(output_destination="logstash_url"), LogstashOutput)

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

            assert isinstance(
                input_sqs.get_output_by_destination(output_destination="elasticsearch_url"), ElasticsearchOutput
            )

        with self.subTest("logstash output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="logstash",
                logstash_url="logstash_url",
                username="username",
                password="password",
                ssl_assert_fingerprint="fingerprint",
            )

            assert isinstance(input_sqs.get_output_by_destination(output_destination="logstash_url"), LogstashOutput)

        with self.subTest("not elasticsearch or logstash output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(ValueError, "another-type"):
                input_sqs.add_output(output_type="another-type")

    def test_get_output_destinations(self) -> None:
        with self.subTest("none output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            assert input_sqs.get_output_destinations() == []

        with self.subTest("elasticsearch output with only elasticsearch_url set"):
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

            assert input_sqs.get_output_destinations() == ["elasticsearch_url"]

        with self.subTest("elasticsearch output with only cloud_id set"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                cloud_id="cloud_id",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert input_sqs.get_output_destinations() == ["cloud_id"]

        with self.subTest("elasticsearch output with elasticsearch_url and cloud_id set"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            input_sqs.add_output(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                cloud_id="cloud_id",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
                batch_max_actions=1,
                batch_max_bytes=1,
            )

            assert input_sqs.get_output_destinations() == ["elasticsearch_url"]

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

            input_sqs.delete_output_by_destination("elasticsearch_url")
            assert input_sqs.get_output_destinations() == []

        with self.subTest("delete not existing output"):
            input_sqs = Input(input_type="s3-sqs", input_id="id")
            with self.assertRaisesRegex(KeyError, "destination"):
                input_sqs.delete_output_by_destination("destination")


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
            with self.assertRaisesRegex(ValueError, "Duplicated input with id id"):
                config.add_input(Input(input_type="s3-sqs", input_id="id"))


@pytest.mark.unit
class TestParseConfig(TestCase):
    def test_parse_config(self) -> None:
        with self.subTest("empty config"):
            with self.assertRaises(AssertionError):
                parse_config(config_yaml="")

            with self.subTest("no inputs"):
                with self.assertRaisesRegex(ValueError, "`inputs` must be provided as list"):
                    parse_config(
                        config_yaml="""
        config:
        """
                    )

                with self.assertRaisesRegex(ValueError, "`inputs` must be provided as list"):
                    parse_config(
                        config_yaml="""
        inputs: {}
                """
                    )

        with self.subTest("no input type"):
            with self.assertRaisesRegex(ValueError, "`type` must be provided as string for input id"):
                parse_config(
                    config_yaml="""
        inputs:
          - id: id
        """
                )

            with self.assertRaisesRegex(ValueError, "`type` must be provided as string for input id"):
                parse_config(
                    config_yaml="""
        inputs:
          - id: id
          - type: {}
        """
                )

        with self.subTest("no input id"):
            with self.assertRaisesRegex(ValueError, "`id` must be provided as string for input at position 1"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: type
            """
                )

        with self.assertRaisesRegex(ValueError, "`id` must be provided as string for input at position 1"):
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
                "^An error occurred while applying type configuration for input id: "
                "`type` must be one of cloudwatch-logs,s3-sqs,sqs,kinesis-data-stream: another-type given$",
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: another-type
                id: id
            """
                )

        with self.subTest("no input output"):
            with self.assertRaisesRegex(ValueError, "`outputs` must be provided as list for input id"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
            """
                )

            with self.assertRaisesRegex(ValueError, "`outputs` must be provided as list for input id"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs: {}
            """
                )

        with self.subTest("no valid input output type"):
            with self.assertRaisesRegex(
                ValueError, "`type` for output configuration at position 1 must be provided as string for input id"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - args: {}
            """
                )

            with self.assertRaisesRegex(
                ValueError, "`type` for output configuration at position 1 must be provided as string for input id"
            ):
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
            with self.assertRaisesRegex(
                ValueError, "`args` for output configuration at position 1 must be provided as dictionary for input id"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                outputs:
                  - type: type
            """
                )

            with self.assertRaisesRegex(
                ValueError, "`args` for output configuration at position 1 must be provided as dictionary for input id"
            ):
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

        with self.subTest("batch_max_actions not int"):
            with self.assertRaisesRegex(
                ValueError,
                "An error occurred while applying output configuration at position 1 for input id: "
                "`batch_max_actions` must be provided as integer",
            ):
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
            with self.assertRaisesRegex(
                ValueError,
                "An error occurred while applying output configuration at position 1 for input id: "
                "`batch_max_bytes` must be provided as integer",
            ):
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

        with self.subTest("ssl_assert_fingerprint not str"):
            with self.assertRaisesRegex(
                ValueError,
                "An error occurred while applying output configuration at position 1 for input id: "
                "`ssl_assert_fingerprint` must be provided as string",
            ):
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
                      ssl_assert_fingerprint: [0, 1]
            """
                )

        with self.subTest("tags not list"):
            with self.assertRaisesRegex(ValueError, "`tags` must be provided as list for input id"):
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

            with self.assertRaisesRegex(ValueError, "`tags` must be provided as list for input id"):
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
                ValueError,
                "Each tag in `tags` must be provided as string for input id, given: "
                "\\[2021, {'key1': 'value1'}, 'tag3'\\]",
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

        with self.subTest("valid expand_event_list_from_field"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                expand_event_list_from_field: aField
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
            assert input_sqs.expand_event_list_from_field == "aField"
            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("expand_event_list_from_field not str"):
            with self.assertRaisesRegex(
                ValueError, "`expand_event_list_from_field` must be provided as string for input id"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                expand_event_list_from_field: 0
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("valid root_fields_to_add_to_expanded_event as `all`"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                root_fields_to_add_to_expanded_event: all
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
            assert input_sqs.root_fields_to_add_to_expanded_event == "all"
            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("valid root_fields_to_add_to_expanded_event as list of strings"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                root_fields_to_add_to_expanded_event: ["one", "two"]
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
            assert input_sqs.root_fields_to_add_to_expanded_event == ["one", "two"]
            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("root_fields_to_add_to_expanded_event not `all` when string"):
            with self.assertRaisesRegex(
                ValueError, "`root_fields_to_add_to_expanded_event` must be provided as `all` or a list of strings"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                root_fields_to_add_to_expanded_event: not_all
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("root_fields_to_add_to_expanded_event not `all` neither list of strings"):
            with self.assertRaisesRegex(
                ValueError, "`root_fields_to_add_to_expanded_event` must be provided as `all` or a list of strings"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                root_fields_to_add_to_expanded_event: 0
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("json_content_type single"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                json_content_type: single
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
            assert input_sqs.json_content_type == "single"

        with self.subTest("json_content_type ndjson"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                json_content_type: ndjson
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
            assert input_sqs.json_content_type == "ndjson"

        with self.subTest("json_content_type disabled"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                json_content_type: disabled
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
            assert input_sqs.json_content_type == "disabled"

        with self.subTest("json_content_type not valid"):
            with self.assertRaisesRegex(
                ValueError, "`json_content_type` must be one of ndjson,single,disabled for input id: whatever given"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                json_content_type: whatever
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("multiline not valid"):
            with self.assertRaisesRegex(ValueError, "`multiline` must be provided as dictionary for input id"):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline: 0
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("multiline type missing"):
            with self.assertRaisesRegex(
                ValueError, "`type` must be provided as string in multiline configuration for input id"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  match: after
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("multiline type not str"):
            with self.assertRaisesRegex(
                ValueError, "`type` must be provided as string in multiline configuration for input id"
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: 0
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

        with self.subTest("multiline type not valid"):
            with self.assertRaisesRegex(
                ValueError,
                "An error occurred while applying multiline configuration for input id: "
                "You must provide one of the following multiline types: count, pattern, while_pattern. "
                "another-type given",
            ):
                parse_config(
                    config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: another-type
                outputs:
                  - type: elasticsearch
                    args:
                      cloud_id: "cloud_id"
                      api_key: "api_key"
                      es_datastream_name: "es_datastream_name"
            """
                )

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="elasticsearch_url")

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="elasticsearch_url")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.elasticsearch_url == "elasticsearch_url"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

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
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["input_tag1", "input_tag2"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == ["tag1", "tag2", "tag3"]
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("no list for include"):
            with self.assertRaisesRegex(ValueError, "`include` must be provided as list for input id"):
                parse_config(
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
            with self.assertRaisesRegex(ValueError, "`exclude` must be provided as list for input id"):
                parse_config(
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

        with self.subTest("valid count multiline with default values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: count
                  count_lines: 1
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
            assert input_sqs.get_multiline_processor() == CountMultiline(count_lines=1)

        with self.subTest("valid count multiline with custom values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: count
                  count_lines: 1
                  max_bytes: 1
                  max_lines: 2
                  skip_newline: true
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
            assert input_sqs.get_multiline_processor() == CountMultiline(
                count_lines=1,
                max_bytes=1,
                max_lines=2,
                skip_newline=True,
            )

        with self.subTest("valid pattern multiline with default values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: pattern
                  pattern: "\\\\$"
                  match: after
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
            assert input_sqs.get_multiline_processor() == PatternMultiline(pattern="\\$", match="after")

        with self.subTest("valid pattern multiline with custom values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: pattern
                  pattern: "\\\\$"
                  match: after
                  negate: true
                  flush_pattern: flush_pattern
                  max_bytes: 1
                  max_lines: 2
                  skip_newline: true
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
            assert input_sqs.get_multiline_processor() == PatternMultiline(
                pattern="\\$",
                match="after",
                negate=True,
                flush_pattern="flush_pattern",
                max_bytes=1,
                max_lines=2,
                skip_newline=True,
            )

        with self.subTest("valid while_pattern multiline with default values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: while_pattern
                  pattern: "\\\\$"
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
            assert input_sqs.get_multiline_processor() == WhileMultiline(pattern="\\$")

        with self.subTest("valid while_pattern multiline with custom values"):
            config = parse_config(
                config_yaml="""
            inputs:
              - type: s3-sqs
                id: id
                multiline:
                  type: while_pattern
                  pattern: "\\\\$"
                  negate: true
                  max_bytes: 1
                  max_lines: 2
                  skip_newline: true
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
            assert input_sqs.get_multiline_processor() == WhileMultiline(
                pattern="\\$",
                negate=True,
                max_bytes=1,
                max_lines=2,
                skip_newline=True,
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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 1
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == ""

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

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 1
            assert elasticsearch.ssl_assert_fingerprint == ""

        with self.subTest("ssl_assert_fingerprint not default"):
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
                      ssl_assert_fingerprint: "2D:4D:CF:FD:6C:2C:00:7E:C3:78:F6:70:A8:F9:34:09:58:6E:40:FC"
            """
            )

            input_sqs = config.get_input_by_id(input_id="id")
            assert input_sqs is not None
            assert input_sqs.type == "s3-sqs"
            assert input_sqs.id == "id"
            assert input_sqs.tags == []

            elasticsearch = input_sqs.get_output_by_destination(output_destination="cloud_id")

            assert elasticsearch is not None
            assert isinstance(elasticsearch, ElasticsearchOutput)
            assert elasticsearch.type == "elasticsearch"
            assert elasticsearch.cloud_id == "cloud_id"
            assert elasticsearch.api_key == "api_key"
            assert elasticsearch.es_datastream_name == "es_datastream_name"
            assert elasticsearch.tags == []
            assert elasticsearch.batch_max_actions == 500
            assert elasticsearch.batch_max_bytes == 10485760
            assert elasticsearch.ssl_assert_fingerprint == "2D:4D:CF:FD:6C:2C:00:7E:C3:78:F6:70:A8:F9:34:09:58:6E:40:FC"
