# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import base64
import json
from typing import Optional
from unittest import TestCase

import mock
import pytest
from botocore.exceptions import ClientError

from share.secretsmanager import aws_sm_expander


@pytest.mark.unit
class MockContent:
    SECRETS_MANAGER_MOCK_DATA: dict[str, dict[str, dict[str, str]]] = {
        "eu-central-1": {
            "es_secrets": {
                "type": "SecretString",
                "data": json.dumps(
                    {
                        "url": "mock_elastic_url",
                        "username": "mock_elastic_username",
                        "password": "mock_elastic_password",
                    }
                ),
            },
            "plain_secret": {"type": "SecretString", "data": "mock_plain_text_sqs_arn"},
        },
        "eu-west-1": {
            "binary_secret": {"type": "SecretBinary", "data": "bW9ja19uZ2lueC5sb2c="},
            "empty_secret": {"type": "SecretString", "data": ""},
        },
    }

    @staticmethod
    def get_secret_values(secret_name: str, region_name: str) -> Optional[str]:
        secret_data: str = ""

        if region_name == "eu-central-1":
            secrets = MockContent.SECRETS_MANAGER_MOCK_DATA[region_name].get(secret_name)

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
                secret_data = base64.b64decode(secrets["data"]).decode("utf-8")
            elif secrets["type"] == "SecretString":
                secret_data = secrets["data"]

        elif region_name == "eu-west-1":
            secrets = MockContent.SECRETS_MANAGER_MOCK_DATA[region_name].get(secret_name)

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
                secret_data = base64.b64decode(secrets["data"]).decode("utf-8")
            elif secrets["type"] == "SecretString":
                secret_data = secrets["data"]

        return secret_data


class TestAWSSecretsManager(TestCase):
    @mock.patch(
        "share.secretsmanager.get_secret_values",
        new=MockContent.get_secret_values,
    )
    def test_parse_secrets_manager(self) -> None:
        with self.subTest("invalid arn format - too long"):
            config_yaml = """
                inputs:
                    - type: sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:sqs_secret:THIS:IS:INVALID"
                    outputs:
                        - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:
                            es_secrets:elasticsearch_url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:
                            es_secrets:elasticsearch_url"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "dataset"
                            namespace: "namespace"
            """

            with self.assertRaisesRegex(
                SyntaxError,
                "Invalid arn format: arn:aws:secretsmanager:eu-central-1:123-456-789:"
                + "secret:sqs_secret:THIS:IS:INVALID",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("invalid arn format - pattern not caught"):
            config_yaml = """
                inputs:
                - type: sqs
                    id: "aws:arn:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secrets_manager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "dataset"
                            namespace: "namespace"
            """
            mock_fetched_data = aws_sm_expander(config_yaml)

            parsed_config_yaml = """
                inputs:
                - type: sqs
                    id: "aws:arn:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secrets_manager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "mock_elastic_username"
                            password: "mock_elastic_password"
                            dataset: "dataset"
                            namespace: "namespace"
            """

            assert mock_fetched_data == parsed_config_yaml

        with self.subTest("invalid both plain text and json secret"):
            config_yaml = """
                inputs:
                - type: sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:sqs_secret"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "dataset"
                            namespace: "namespace"
            """

            with self.assertRaisesRegex(
                ValueError,
                "You cannot have both plain text and json key for the same secret: "
                + "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("secret does not exist"):
            config_yaml = """
                inputs:
                - type: sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:DOES_NOT_EXIST"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "dataset"
                            namespace: "namespace"
            """

            with self.assertRaises(ClientError):
                aws_sm_expander(config_yaml)

        with self.subTest("secret is empty"):
            config_yaml = """
                inputs:
                - type: sqs
                    id: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:empty_secret"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "dataset"
                            namespace: "namespace"
            """

            with self.assertRaisesRegex(
                ValueError,
                "Value for secret: arn:aws:secretsmanager:eu-west-1:123-456-789:secret:empty_secret must not be empty",
            ):
                aws_sm_expander(config_yaml)

        with self.subTest("config successfully parsed"):
            config_yaml = """
                inputs:
                - type: sqs
                    id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_secret"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:url"
                            username: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:username"
                            password: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_secrets:password"
                            dataset: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:binary_secret"
                            namespace: "namespace"
            """
            mock_fetched_data = aws_sm_expander(config_yaml)

            parsed_config_yaml = """
                inputs:
                - type: sqs
                    id: "mock_plain_text_sqs_arn"
                    outputs:
                    - type: elasticsearch
                        args:
                            elasticsearch_url: "mock_elastic_url"
                            username: "mock_elastic_username"
                            password: "mock_elastic_password"
                            dataset: "mock_nginx.log"
                            namespace: "namespace"
            """

            assert mock_fetched_data == parsed_config_yaml
