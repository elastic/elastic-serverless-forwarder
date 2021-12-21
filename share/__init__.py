# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from .config import Config, ElasticsearchOutput, Input, Output, parse_config
from .logger import logger as shared_logger
from .secretsmanager import aws_sm_expander
