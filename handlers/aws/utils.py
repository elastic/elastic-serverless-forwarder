_available_triggers: dict[str, str] = {"aws:sqs": "sqs"}


def _from_s3_uri_to_bucket_name_and_object_key(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    s3_uri = s3_uri.strip("s3://")

    bucket_name_and_object_key = s3_uri.split("/", 1)
    if len(bucket_name_and_object_key) != 2:
        raise ValueError(f"Invalid s3 uri provided: {s3_uri}")

    return bucket_name_and_object_key[0], bucket_name_and_object_key[1]


def _enrich_event(event: dict[str, any], event_type: str, dataset: str, namespace: str):
    event["data_stream"] = {
        "type": event_type,
        "dataset": dataset,
        "namespace": namespace,
    }

    event["event"] = {"dataset": dataset, "original": event["fields"]["message"]}

    event["tags"] = ["preserve_original_event", "forwarded", dataset.replace(".", "-")]


def _get_bucket_name_from_arn(bucket_arn: str) -> str:
    return bucket_arn.split(":")[-1]


def _get_trigger_type(event: dict[str, any]) -> str:
    if "Records" in event and len(event["Records"]) > 0:
        if "eventSource" in event["Records"][0]:
            event_source = event["Records"][0]["eventSource"]
            if event_source in _available_triggers:
                return _available_triggers[event_source]

    raise Exception("not supported trigger")
