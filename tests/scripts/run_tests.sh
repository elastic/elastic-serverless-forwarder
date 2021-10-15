#!/usr/bin/env bash

set -e

if [[ "$WITH_COVERAGE" == "true" ]]
then
    make coverage
else
    make test
fi
