# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.


class TriggerTypeException(Exception):
    """Raised when there is an error related to the trigger type"""

    pass


class ConfigFileException(Exception):
    """Raised when there is an error related to the config file"""

    pass


class InputConfigException(Exception):
    """Raised when there is an error related to the configured input"""

    pass


class OutputConfigException(Exception):
    """Raised when there is an error related to the configured output"""

    pass


class ReplayHandlerException(Exception):
    """Raised when there is an error in ingestion in the replay queue"""

    pass
