#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.12 -t lint_mypy --file Dockerfile ..
docker run \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -v ${pip_cache}:$(dirname ${docker_pip_cache}) \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm lint_mypy \
  /bin/bash \
  -c "pip install --user -U pip
      pip install --user -r requirements-lint.txt --cache-dir ${docker_pip_cache}
      pip install --user -r requirements-tests.txt --cache-dir ${docker_pip_cache}
      pip install --user -r requirements.txt --cache-dir ${docker_pip_cache}
      PATH=\${PATH}:\${HOME}/.local/bin/ /bin/bash ./tests/scripts/mypy.sh $*"
