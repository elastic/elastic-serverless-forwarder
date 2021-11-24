#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -ex

# delete any __pycache__ folders to avoid hard-to-debug caching issues
find . -name __pycache__ -type d -exec rm -r {} +
py.test -v ${PYTEST_ARGS} "${PYTEST_JUNIT}" tests

if [[ "${PYTEST_ARGS}" == *"--cov"* ]]; then
    # Transform coverage to xml so Jenkins can parse and report it
    coverage xml
    coverage html
fi
