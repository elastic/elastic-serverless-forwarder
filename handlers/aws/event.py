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
        "aws": {
            "s3": {
                "bucket": {
                    "name": "",
                    "arn": "",
                },
                "object": {
                    "key": "",
                },
            },
        },
        "cloud": {
            "provider": "aws",
            "region": "",
        },
    },
}
