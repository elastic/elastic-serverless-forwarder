#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
set -e
if [[ $# -eq 0 ]]
then
    echo "Usage: $0 check|fix"
    exit 1
fi

if [[ "$1" = "check" ]]
then
    OPTIONS="--check "
elif [[ "$1" = "fix" ]]
then
    OPTIONS="--fix"
else
    echo "You have to run the script with either 'check' or 'fix' args"
    exit 1
fi

python tests/scripts/parse_notice.py ${OPTIONS}
