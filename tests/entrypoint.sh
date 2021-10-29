#!/usr/bin/env bash

export HOME=/home/user
exec /usr/local/bin/gosu user "$@"
