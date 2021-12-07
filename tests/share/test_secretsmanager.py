# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import base64
import json
from typing import Optional, Union
from unittest import TestCase

import mock
import pytest
from botocore.exceptions import ClientError

from share.secretsmanager import aws_sm_expander


@pytest.mark.unit
class MockContent:
    SECRETS_MANAGER_MOCK_DATA: dict[str, dict[str, str]] = {
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
        "binary_secret": {"type": "SecretBinary", "data": "bW9ja19uZ2lueC5sb2c="},
        "empty_secret": {"type": "SecretString", "data": ""},
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


class TestAWSSecretsManager(TestCase):
    @mock.patch("share.secretsmanager._get_aws_sm_client", new=MockContent._get_aws_sm_client)
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
            # BEWARE aws:arn != arn:aws at id
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
