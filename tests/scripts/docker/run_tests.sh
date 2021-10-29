#!/usr/bin/env bash
set -ex

pip_cache="$HOME/.cache"
docker_pip_cache="/tmp/cache/pip"

cd tests

docker network create run_tests || true
docker build --build-arg UID=$UID --build-arg PYTHON_IMAGE=python:3.9 -t run_tests .
docker run \
  --privileged \
  -e LOCAL_USER_ID=$UID \
  -e PIP_CACHE=${docker_pip_cache} \
  -e PYTEST_ARGS="${PYTEST_ARGS}" \
  -e PYTEST_JUNIT="--junitxml=/app/tests/elastic-serverless-forwarder-junit.xml" \
  -e AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY \
  -v "$(dirname $(pwd))":/app \
  -w /app \
  --rm run_tests \
  /bin/bash \
  -c "sudo service docker start
      pip install --user -U pip
      pip install --user -r tests/requirements/test-reqs.txt --cache-dir ${docker_pip_cache}
      timeout 5m /bin/bash ./tests/scripts/run_tests.sh"

docker network rm run_tests || true
