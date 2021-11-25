#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e
if [[ $# -eq 0 ]]
then
    echo "Usage: $0 diff|fix"
    exit 1
fi

if [[ "$1" = "diff" ]]
then
    OPTIONS="--diff --check --py 39 --profile black --line-length=120"
elif [[ "$1" = "fix" ]]
then
    OPTIONS="-v --py 39 --profile black --line-length=120"
fi

isort ${OPTIONS} .
