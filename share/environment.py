import os
import sys


def is_aws() -> bool:
    return os.getenv('AWS_EXECUTION_ENV') is not None


def environment() -> str:
    if is_aws():
        return os.getenv('AWS_EXECUTION_ENV')
    else:
        return sys.version
