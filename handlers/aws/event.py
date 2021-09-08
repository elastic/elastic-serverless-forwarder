_default_event: dict[str, any] = {
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
