# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import hashlib
import os

from share.version import version


def get_hex_prefix(src: str) -> str:
    return hashlib.sha3_384(src.encode("utf-8")).hexdigest()


def create_user_agent() -> str:
    """Creates the 'User-Agent' header given the library name and version"""
    return f"ElasticServerlessForwarder/{version} ({os.getenv('AWS_EXECUTION_ENV')}; OTHER)"
