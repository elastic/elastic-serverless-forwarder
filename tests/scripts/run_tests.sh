#!/usr/bin/env bash

set -e

export PATH=${HOME}/.local/bin:${PATH}
python -m pip install --user -U pip --cache-dir "${PIP_CACHE}"
python -m pip install --user -r "tests/requirements/requirements.txt" --cache-dir "${PIP_CACHE}"

if [[ "$WITH_COVERAGE" == "true" ]]
then
    make coverage
else
    make test
fi
