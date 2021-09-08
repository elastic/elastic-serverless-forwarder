import datetime


_default_event: dict[str, any] = {
    "timestamp": datetime.date.today(),
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
