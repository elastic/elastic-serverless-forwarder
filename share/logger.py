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
logger.addHandler(handler)
