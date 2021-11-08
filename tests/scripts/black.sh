#!/usr/bin/env bash
echo ""
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

black -t py39 ${OPTIONS} /app
