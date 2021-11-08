#!/usr/bin/env bash

set -ex

# delete any __pycache__ folders to avoid hard-to-debug caching issues
find . -name __pycache__ -type d -exec rm -r {} +
py.test -v "${PYTEST_ARGS}" "${PYTEST_JUNIT}" tests
# Transform coverage to xml so Jenkins can parse and report it
coverage xml
coverage html
