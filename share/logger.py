# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import logging
import os
from typing import Any

import ecs_logging
from elasticapm.handlers.logging import LoggingFilter

# AWS CloudWatch Log Level Filtering requires a root-level "level" field.
# Python's WARNING and CRITICAL don't match AWS's expected values.
_AWS_LEVEL_MAP = {
    "WARNING": "WARN",
    "CRITICAL": "FATAL",
}


class AWSCompatibleFormatter(ecs_logging.StdlibFormatter):
    """ECS formatter that also emits a root-level "level" field.

    AWS CloudWatch Logs Log Level Filtering requires a top-level "level" field
    in JSON-structured log output. ECS emits severity as the flat dotted key
    "log.level", which AWS does not recognise for filtering purposes.
    """

    def format_to_ecs(self, record: logging.LogRecord) -> dict[str, Any]:
        result = super().format_to_ecs(record)
        result["level"] = _AWS_LEVEL_MAP.get(record.levelname, record.levelname)
        return result


log_level = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO").upper())

# Get the Logger
logger = logging.getLogger()
logger.setLevel(log_level)
logger.propagate = False

# Add an ECS formatter to the Handler
handler = logging.StreamHandler()
handler.setFormatter(AWSCompatibleFormatter())

# Add an APM log correlation
handler.addFilter(LoggingFilter())  # type: ignore
logger.handlers = [handler]
