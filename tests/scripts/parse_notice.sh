#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

if [[ $# -eq 0 ]]
then
    echo "Usage: $0 scanned_file_name check|fix"
    exit 1
fi

SCANNED_FILE_NAME=$1

if [[ "$2" = "check" ]]
then
    MODE="--check"
elif [[ "$2" = "fix" ]]
then
    MODE="--fix"
else
    echo "You have to run the script with either 'check' or 'fix' args"
    exit 1
fi

python tests/scripts/parse_notice.py ${SCANNED_FILE_NAME} ${MODE}
