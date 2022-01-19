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

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.9 -t python-linters --file Dockerfile ..
docker run \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -v ${pip_cache}:$(dirname ${docker_pip_cache}) \
  -v "$(dirname $(pwd))":/app \
  --rm python-linters \
  /bin/bash \
  -c "python3 -m pip install -r requirements.txt
      python3 -m pip install -r requirements-tests.txt
      python3 -m pip install -r requirements-lint.txt
      scancode -clpi -n 16 --include \"*LICENSE*\" --include \"*METADATA*\" --max-depth 6 --json-pp ${SCANNED_FILE_NAME} .
      ./tests/scripts/parse_notice.sh ${SCANNED_FILE_NAME} ${MODE}
  "
