#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
set -ex

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.9 -t run_tests --file Dockerfile ..
docker run \
  --privileged \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -e PYTEST_ARGS="${PYTEST_ARGS}" \
  -e PYTEST_ADDOPTS="${PYTEST_ADDOPTS}" \
  -e PYTEST_JUNIT="--junitxml=/app/tests/elastic-serverless-forwarder-junit.xml" \
  -e AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm run_tests \
  /bin/bash \
  -c "sudo service docker start
      pip install --user -U pip
      pip install --user -r requirements-tests.txt --cache-dir ${docker_pip_cache}
      pip install --user -r requirements.txt --cache-dir ${docker_pip_cache}
      PATH=\${PATH}:\${HOME}/.local/bin/ timeout 60m /bin/bash ./tests/scripts/run_tests.sh"
