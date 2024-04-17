# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any

from aws_lambda_typing import context as context_

from handlers.aws import lambda_handler


def handler(lambda_event: dict[str, Any], lambda_context: context_.Context) -> Any:
    """
    AWS Lambda handler as main entrypoint
    This is just a wrapper to handlers.aws.lambda_handler
    """
    return lambda_handler(lambda_event, lambda_context)
