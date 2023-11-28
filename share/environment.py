# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
import platform


def is_aws() -> bool:
    return os.getenv("AWS_EXECUTION_ENV") is not None


def get_environment() -> str:
    if is_aws():
        return os.environ["AWS_EXECUTION_ENV"]
    else:
        return f"Python/{platform.python_version()} {platform.system()}/{platform.machine()}"
