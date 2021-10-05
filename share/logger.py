# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import logging

import ecs_logging
from elasticapm.handlers.logging import LoggingFilter

# Get the Logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.propagate = False

# Add an ECS formatter to the Handler
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())

# Add an APM log correlation
handler.addFilter(LoggingFilter())
logger.handlers = [handler]
