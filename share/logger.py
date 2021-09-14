import logging

import ecs_logging
from elasticapm.handlers.logging import LoggingFilter


def get_logger(logger_name: str) -> logging.Logger:
    # Get the Logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Add an ECS formatter to the Handler
    handler = logging.StreamHandler()
    handler.setFormatter(ecs_logging.StdlibFormatter())
    handler.addFilter(LoggingFilter())
    logger.addHandler(handler)

    return logger
