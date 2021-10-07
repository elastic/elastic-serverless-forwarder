# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any

from .logger import logger


def wrap_try_except(func):
    def wrapper(*args) -> Any:
        try:
            return func(*args)
        except Exception as e:
            logger.exception("exception raised", exc_info=e)
            return str(e)

    return wrapper
