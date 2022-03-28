# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any

_default_event: dict[str, Any] = {
    "@timestamp": "",
    "fields": {
        "message": "",
        "log": {
            "offset": 0,
            "file": {
                "path": "",
            },
        },
        "aws": {},
        "cloud": {
            "provider": "aws",
            "region": "",
        },
    },
    "meta": {},
}
