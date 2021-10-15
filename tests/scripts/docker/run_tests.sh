#!/usr/bin/env bash
set -ex

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker build --build-arg PYTHON_IMAGE=python:3.9 -t run_tests .
docker run \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -e WITH_COVERAGE=true \
  -e PYTEST_JUNIT="--junitxml=/app/tests/elastic-serverless-agent-junit.xml" \
  -v ${pip_cache}:$(dirname ${docker_pip_cache}) \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm run_tests \
  /bin/bash \
  -c "pip install --user -U pip
      pip install --user -r tests/requirements/requirements.txt --cache-dir ${docker_pip_cache}
      timeout 5m /bin/bash ./tests/scripts/run_tests.sh"
