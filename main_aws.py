from typing import Any

from aws_lambda_typing import context as context_

from handlers.aws import lambda_handler


def handler(lambda_event: dict[str, Any], lambda_context: context_.Context) -> str:
    return lambda_handler(lambda_event, lambda_context)
