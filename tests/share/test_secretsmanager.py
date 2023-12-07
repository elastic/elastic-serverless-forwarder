# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import base64
from typing import Optional, Union
from unittest import TestCase

import mock
import pytest
from botocore.client import BaseClient as BotoBaseClient
from botocore.exceptions import ClientError, InvalidRegionError

from share import json_dumper
from share.secretsmanager import _get_aws_sm_client, aws_sm_expander


class MockContent:
    SECRETS_MANAGER_MOCK_DATA: dict[str, dict[str, str]] = {
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets": {
            "type": "SecretString",
            "data": json_dumper(
                {
                    "url": "mock_elastic_url",
                    "username": "mock_elastic_username",
                    "password": "mock_elastic_password",
                    "empty": "",
                }
            ),
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret": {
            "type": "SecretString",
            "data": "mock_plain_text_sqs_arn",
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte": {
            "type": "SecretString",
            "data": b"i am not a str",  # type:ignore
        },
        "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int": {
            "type": "SecretString",
            "data": 2021,  # type:ignore
        },
        "arn:aws:secretsmanager:eu-west-1:123456789:secret:binary_secret": {
            "type": "SecretBinary",
            "data": "bW9ja19uZ2lueC5sb2c=",
        },
        "arn:aws:secretsmanager:eu-west-1:123456789:secret:empty_secret": {"type": "SecretString", "data": ""},
    }

    @staticmethod
    def _get_aws_sm_client(region_name: str) -> mock.MagicMock:
        client = mock.Mock()
        client.get_secret_value = MockContent.get_secret_value
        return client

    @staticmethod
    def get_secret_value(SecretId: str) -> Optional[dict[str, Union[bytes, str]]]:
        secrets = MockContent.SECRETS_MANAGER_MOCK_DATA.get(SecretId)

        if secrets is None:
            raise ClientError(
                {
                    "Error": {
                        "Message": "Secrets Manager can't find the specified secret.",
                        "Code": "ResourceNotFoundException",
                    }
                },
                "GetSecretValue",
            )

        if secrets["type"] == "SecretBinary":
            return {"SecretBinary": base64.b64decode(secrets["data"])}
        elif secrets["type"] == "SecretString":
            return {"SecretString": secrets["data"]}

        return None


@pytest.mark.unit
class TestAWSSecretsManager(TestCase):
    @mock.patch("share.secretsmanager._get_aws_sm_client", new=MockContent._get_aws_sm_client)
    def test_parse_secrets_manager(self) -> None:
        with self.subTest("invalid arn format - too long"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:THIS:IS:INVALID"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                SyntaxError,
                "Invalid arn format: arn:aws:secretsmanager:eu-central-1:123456789:"
                + "secret:plain_secret:THIS:IS:INVALID",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("region is empty"):
            # BEWARE empty region at id
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager::123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """
            with self.assertRaisesRegex(
                ValueError, "Must be provided region in arn: arn:aws:secretsmanager::123456789:secret:plain_secret"
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("empty secrets manager name"):
            # BEWARE empty secrets manager name at id
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """
            with self.assertRaisesRegex(
                ValueError,
                "Must be provided secrets manager name in arn: arn:aws:secretsmanager:eu-central-1:123456789:secret:",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("empty secret key"):
            # BEWARE empty secret key at elasticsearch_url
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """
            with self.assertRaisesRegex(
                ValueError,
                "Error for secret arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:: "
                "key must not be empty",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("invalid both plain text and json secret"):
            # BEWARE elasticsearch_url and password have json key, but username don't
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:sqs_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                ValueError,
                "You cannot have both plain text and json key for the same secret: "
                + "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("secret does not exist"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:DOES_NOT_EXIST"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaises(ClientError):
                aws_sm_expander(config_yaml)

        with self.subTest("plain secret is empty"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-west-1:123456789:secret:empty_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                ValueError,
                "Error for secret arn:aws:secretsmanager:eu-west-1:123456789:secret:empty_secret: must not be empty",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("key/value secret is empty"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:empty"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                ValueError,
                "Error for secret arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:empty: "
                "must not be empty",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("plain text used as key/value pair"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:I_SHOULD_NOT_HAVE_A_KEY"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                ValueError,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret:I_SHOULD_NOT_HAVE_A_KEY: "
                "expected to be keys/values pair",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("key does not exist"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:I_DO_NOT_EXIST"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                KeyError,
                "Error for secret arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:I_DO_NOT_EXIST: "
                "key not found",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("plain text secret not str"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                ValueError,
                "Error for secret arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_byte: "
                "expected to be a string",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("json TypeError risen"):
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """

            with self.assertRaisesRegex(
                Exception,
                "Error for secret "
                "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret_not_str_int: "
                "expected to be a string",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("invalid arn format - pattern not caught"):
            # BEWARE aws:arn != arn:aws at id
            # BEWARE secrets_manager != secretsmanager at elasticsearch_url
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "aws:arn:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secrets_manager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """
            mock_fetched_data = aws_sm_expander(config_yaml)

            parsed_config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "aws:arn:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secrets_manager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "mock_elastic_username"
                            password: "mock_elastic_password"
                            es_datastream_name: "es_datastream_name"
            """

            assert mock_fetched_data == parsed_config_yaml

        with self.subTest("invalid arn format - pattern not caught: case sensitive"):
            # BEWARE AWS != aws at id
            config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:AWS:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                            es_datastream_name: "es_datastream_name"
            """
            mock_fetched_data = aws_sm_expander(config_yaml)

            parsed_config_yaml = """
                inputs:
                  - type: s3-sqs
                    id: "arn:AWS:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                    outputs:
                      - type: elasticsearch
                        args:
                            elasticsearch_url: "mock_elastic_url"
                            username: "mock_elastic_username"
                            password: "mock_elastic_password"
                            es_datastream_name: "es_datastream_name"
            """

            assert mock_fetched_data == parsed_config_yaml

        with self.subTest("config successfully parsed"):
            config_yaml = """
              inputs:
                - type: s3-sqs
                  id: "arn:aws:secretsmanager:eu-central-1:123456789:secret:plain_secret"
                  outputs:
                    - type: elasticsearch
                      args:
                        elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:url"
                        username: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:username"
                        password: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_secrets:password"
                        es_datastream_name: "arn:aws:secretsmanager:eu-west-1:123456789:secret:binary_secret"
            """
            mock_fetched_data = aws_sm_expander(config_yaml)

            parsed_config_yaml = """
              inputs:
                - type: s3-sqs
                  id: "mock_plain_text_sqs_arn"
                  outputs:
                    - type: elasticsearch
                      args:
                        elasticsearch_url: "mock_elastic_url"
                        username: "mock_elastic_username"
                        password: "mock_elastic_password"
                        es_datastream_name: "mock_nginx.log"
            """

            assert mock_fetched_data == parsed_config_yaml

    def test_get_aws_sm_client(self) -> None:
        with self.subTest("client is of type baseclient"):
            region_name: str = "eu-central-1"
            assert isinstance(_get_aws_sm_client(region_name), BotoBaseClient)

        with self.subTest("invalid region format"):
            region_name = "22222222222"
            with self.assertRaisesRegex(
                InvalidRegionError,
                "Provided region_name '22222222222' doesn't match a supported format.",
            ):
                _get_aws_sm_client(region_name)
