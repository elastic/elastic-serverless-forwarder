# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from typing import Any, Union

import boto3
from botocore.client import BaseClient as BotoBaseClient
from orjson import JSONDecodeError

from .json import json_parser
from .logger import logger as shared_logger


def _get_aws_sm_client(region_name: str) -> BotoBaseClient:
    """
    Getter for secrets manager client
    Extracted for mocking
    """

    return boto3.client("secretsmanager", region_name=region_name)


def aws_sm_expander(config_yaml: str) -> str:
    """
    Secrets Manager expander for config file
    It scans the file for the secrets manager arn pattern, checks for correct configuration,
    retrieves the values from the secret manager and replaces them in the config file.
    Exceptions will be raised for the following scenarios:
        - Not respecting the arn pattern
        - Input is for both plain text and json keys for the same secret manager name
        - The fetched value is empty
    """

    config_secret_entry_values: dict[str, str] = {}
    secret_arn_by_secret_name: dict[str, str] = {}
    secret_key_values_cache: dict[str, dict[str, Any]] = {}
    secret_consistency_len_check: dict[str, int] = {}

    re_pattern = r"arn:aws:secretsmanager:(?:[^:]+)?:(?:[^:]+)?:secret:(?:[^\"']+)?"
    found_secrets_entries = re.findall(re_pattern, config_yaml)

    for secret_arn in found_secrets_entries:
        splitted_secret_arn = secret_arn.split(":")

        if len(splitted_secret_arn) != 7 and len(splitted_secret_arn) != 8:
            raise SyntaxError("Invalid arn format: {}".format(secret_arn))

        if secret_arn not in config_secret_entry_values:
            config_secret_entry_values[secret_arn] = ""

        region = splitted_secret_arn[3]
        secrets_manager_name = splitted_secret_arn[6]

        if region == "":
            raise ValueError("Must be provided region in arn: {}".format(secret_arn))

        if secrets_manager_name == "":
            raise ValueError("Must be provided secrets manager name in arn: {}".format(secret_arn))

        if secrets_manager_name not in secret_consistency_len_check:
            secret_consistency_len_check[secrets_manager_name] = len(splitted_secret_arn)
        else:
            if secret_consistency_len_check[secrets_manager_name] != len(splitted_secret_arn):
                raise ValueError(
                    "You cannot have both plain text and json key for the same secret: {}".format(secret_arn)
                )

        if region not in secret_key_values_cache:
            secret_key_values_cache[region] = {}

        if secrets_manager_name not in secret_key_values_cache[region]:
            secret_key_values_cache[region][secrets_manager_name] = {}

        secret_arn_by_secret_name[secrets_manager_name] = ":".join(splitted_secret_arn[0:7])

    for region in secret_key_values_cache:
        for secrets_manager_name in secret_key_values_cache[region]:
            secret_arn = secret_arn_by_secret_name[secrets_manager_name]
            str_secrets = get_secret_values(secret_arn, region)
            parsed_secrets = parse_secrets_str(str_secrets, secret_arn)

            secret_key_values_cache[region][secrets_manager_name] = parsed_secrets

    for config_secret_entry in config_secret_entry_values:
        splitted_secret_arn = config_secret_entry.split(":")

        region = splitted_secret_arn[3]
        secrets_manager_name = splitted_secret_arn[6]

        if len(splitted_secret_arn) == 8:
            wanted_key = splitted_secret_arn[-1]
            if wanted_key == "":
                raise ValueError(f"Error for secret {config_secret_entry}: key must not be empty")

            if not isinstance(secret_key_values_cache[region][secrets_manager_name], dict):
                raise ValueError(f"Error for secret {config_secret_entry}: expected to be keys/values pair")

            if wanted_key in secret_key_values_cache[region][secrets_manager_name]:
                fetched_secret_entry_value = secret_key_values_cache[region][secrets_manager_name][wanted_key]
                if fetched_secret_entry_value == "":
                    raise ValueError(f"Error for secret {config_secret_entry}: must not be empty")
                config_secret_entry_values[config_secret_entry] = fetched_secret_entry_value
            else:
                raise KeyError(f"Error for secret {config_secret_entry}: key not found")
        else:
            if secret_key_values_cache[region][secrets_manager_name] == "":
                raise ValueError(f"Error for secret {config_secret_entry}: must not be empty")
            elif not isinstance(secret_key_values_cache[region][secrets_manager_name], str):
                raise ValueError(f"Error for secret {config_secret_entry}: expected to be a string")

            config_secret_entry_values[config_secret_entry] = secret_key_values_cache[region][secrets_manager_name]

        config_yaml = config_yaml.replace(config_secret_entry, config_secret_entry_values[config_secret_entry])

    return config_yaml


def get_secret_values(secret_arn: str, region_name: str) -> str:
    """
    Calls the get_secret_value api from secrets manager, and returns the values.
    If the secret is created in a binary format, it will be received as a byte string
    on the "BinarySecret" key (boto3 does the base64 decoding internally).
    Raises exceptions for ClientError errors.
    """

    secrets: str = ""
    client = _get_aws_sm_client(region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
    except Exception as e:
        raise e
    else:
        if "SecretString" in get_secret_value_response:
            secrets = get_secret_value_response["SecretString"]

        else:
            secrets = get_secret_value_response["SecretBinary"].decode("utf-8")

    return secrets


def parse_secrets_str(secrets: str, secret_arn: str) -> Union[str, dict[str, Any]]:
    """
    Helper function to determine if the secrets from secrets manager are json or plain text.
    Returns str or dict only.
    """

    try:
        parsed_secrets: dict[str, str] = json_parser(secrets)
    except JSONDecodeError:
        shared_logger.debug("parsed secrets as plaintext")
        return secrets
    else:
        shared_logger.debug("parsed secrets as json")
        return parsed_secrets
