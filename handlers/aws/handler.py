import datetime
import json
import os

from shippers.shipper import Shipper
from storage.storage import Storage

default_event: dict[str, any] = {
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


def handler_name(event, context):
    _available_triggers: dict[str, str] = {
        "aws:sqs": "sqs"
    }

    def _get_trigger_type(_event) -> str:
        if "Records" in _event and len(_event["Records"]) > 0:
            if "eventSource" in _event["Records"][0]:
                event_source = _event["Records"][0]["eventSource"]
                if event_source in _available_triggers:
                    return _available_triggers[event_source]

        raise Exception("not supported trigger")

    try:
        shipper: Shipper = Shipper(target="elasticsearch",
                                   hosts=os.getenv("HOSTS"),
                                   username=os.getenv("USERNAME"),
                                   password=os.getenv("PASSWORD"),
                                   scheme=os.getenv("SCHEME"),
                                   index=os.getenv("INDEX"),
                                   )

        trigger_type: str = _get_trigger_type(event)

        bucket_arn: str = ""
        object_key: str = ""
        if trigger_type == "sqs":
            for record in event["Records"]:
                bucket_arn = record["s3"]["bucket"]["arn"]
                object_key = record["s3"]["object"]["key"]

            if len(bucket_arn) == 0 or len(object_key) == 0:
                raise Exception("cannot find bucket_arn or object_key for s3")

            bucket_name = bucket_arn.split(":")[-1]
            storage: Storage = Storage(storage_type="s3", bucket_arn=bucket_arn, object_key=object_key)

            events, content_type = storage.get()
            for (event, offset) in events:
                default_event["timestamp"] = ""
                default_event["fields"]["message"] = str(event)
                default_event["fields"]["log"]["offset"] = offset

                default_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{1}".format(
                    bucket_name,
                    object_key
                )

                default_event["fields"]["aws"]["s3"] = {
                    "bucket": {"name": bucket_name, "arn": bucket_arn},
                    "object": {"key": object_key},
                }

                default_event["cloud"]["region"] = ""

                shipper.send(default_event)

    except Exception as e:
        return str(e)
