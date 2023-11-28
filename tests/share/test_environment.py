# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from unittest import mock

import pytest

from share.environment import get_environment


@pytest.mark.unit
@mock.patch.dict(os.environ, {"AWS_EXECUTION_ENV": "AWS_Lambda_Python3.9"})
def test_aws_environment() -> None:
    environment = get_environment()
    assert environment == "AWS_Lambda_Python3.9"
