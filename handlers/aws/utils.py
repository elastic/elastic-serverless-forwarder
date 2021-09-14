from typing import Any

_available_triggers: dict[str, str] = {"aws:sqs": "sqs"}


def from_s3_uri_to_bucket_name_and_object_key(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    s3_uri = s3_uri.strip("s3://")

    bucket_name_and_object_key = s3_uri.split("/", 1)
    if len(bucket_name_and_object_key) != 2:
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    return bucket_name_and_object_key[0], bucket_name_and_object_key[1]


def get_bucket_name_from_arn(bucket_arn: str) -> str:
    return bucket_arn.split(":")[-1]


def get_trigger_type(event: dict[str, Any]) -> str:
    if "Records" not in event and len(event["Records"]) < 1:
        raise Exception("Not supported trigger")

    if "eventSource" not in event["Records"][0]:
        raise Exception("Not supported trigger")

    event_source = event["Records"][0]["eventSource"]
    if event_source not in _available_triggers:
        raise Exception("Not supported trigger")

    trigger_type = _available_triggers[event_source]
    if trigger_type != "sqs":
        return trigger_type

    if "messageAttributes" not in event["Records"][0]:
        return trigger_type

    if "originalEventSource" not in event["Records"][0]["messageAttributes"]:
        return trigger_type

    return "self_sqs"
