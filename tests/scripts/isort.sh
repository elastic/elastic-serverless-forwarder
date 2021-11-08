#!/usr/bin/env bash

set -e
if [[ $# -eq 0 ]]
then
    echo "Usage: $0 diff|fix"
    exit 1
fi

if [[ "$1" = "diff" ]]
then
    OPTIONS="--diff --check --profile black --line-length=120"
elif [[ "$1" = "fix" ]]
then
    OPTIONS="--profile black --line-length=120"
fi


isort "${OPTIONS}" .
