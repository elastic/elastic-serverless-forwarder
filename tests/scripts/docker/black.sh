#!/usr/bin/env bash

set -e
if [[ $# -eq 0 ]]
then
    echo "Usage: $0 diff|fix"
    exit 1
fi

if [[ "$1" = "diff" ]]
then
    OPTIONS="--diff --check --line-length=120"
elif [[ "$1" = "fix" ]]
then
    OPTIONS="--line-length=120"
fi

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.9 -t python-linters .
docker run \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -v ${pip_cache}:$(dirname ${docker_pip_cache}) \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm python-linters \
  /bin/bash \
  -c "pip install --user -U pip
      pip install --user -r tests/requirements/lint-black.txt --cache-dir ${docker_pip_cache}
      \${HOME}/.local/bin/black -t py39 ${OPTIONS} /app"
