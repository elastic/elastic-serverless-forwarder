#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

if [[ $# -eq 0 ]]
then
    echo "Usage: $0 %scanned_file_name% check|fix"
    exit 1
fi

SCANNED_FILE_NAME=$1
touch $SCANNED_FILE_NAME

if [[ "$2" = "check" ]]
then
    MODE="check"
elif [[ "$2" = "fix" ]]
then
    MODE="fix"
else
    echo "You have to run the script in either 'check' or 'fix' mode"
    exit 1
fi

/bin/bash -c "
    python -m pip install --ignore-installed --user --upgrade pip
    python -m pip install --ignore-installed --user -r requirements.txt
    python -m pip install --ignore-installed --user -r requirements-lint.txt
    python -m pip install --ignore-installed --user -r requirements-tests.txt

    export PATH=\${PATH}:\${HOME}/.local/bin/
    scancode -clpi -n 16 --include \"*METADATA*\" --max-depth 5 --full-root --json-pp ${SCANNED_FILE_NAME} \${HOME}/.local/

    python tests/scripts/notice_generator.py -f ${SCANNED_FILE_NAME} -m ${MODE}

    rm -rf \${HOME}/.local/
"
