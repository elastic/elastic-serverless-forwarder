#!/usr/bin/env bash

set -ex

# delete any __pycache__ folders to avoid hard-to-debug caching issues
find . -name __pycache__ -type d -exec rm -r {} +
/home/user/.local/bin/py.test -v ${PYTEST_ARGS} "${PYTEST_JUNIT}" tests
# Transform coverage to xml so Jenkins can parse and report it
/home/user/.local/bin/coverage xml
/home/user/.local/bin/coverage html
