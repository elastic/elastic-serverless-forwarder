# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
import hashlib
import sys


def get_hex_prefix(src: str) -> str:
    return hashlib.sha3_384(src.encode("utf-8")).hexdigest()


def create_user_agent(esf_version: str, environment: str = sys.version) -> str:
    """Creates the 'User-Agent' header given ESF version and running environment"""
    return f"ElasticServerlessForwarder/{esf_version} ({environment})"
