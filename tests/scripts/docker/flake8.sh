#!/usr/bin/env bash

set -e

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.9 -t lint_flake8 --file Dockerfile ..
docker run \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -v ${pip_cache}:$(dirname ${docker_pip_cache}) \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm lint_flake8 \
  /bin/bash \
  -c "pip install --user -U pip
      pip install --user -r requirements-lint.txt --cache-dir ${docker_pip_cache}
      PATH=\${PATH}:\${HOME}/.local/bin/ /bin/bash ./tests/scripts/flake8.sh $*"
