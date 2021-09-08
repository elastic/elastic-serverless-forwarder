_available_triggers: dict[str, str] = {
    "aws:sqs": "sqs"
}


def _enrich_event(event: dict[str, any], event_type: str, dataset: str, namespace: str):
    event["data_stream"] = {
        "type": event_type,
        "dataset": dataset,
        "namespace": namespace,
    }

    event["event"] = {
        "dataset": dataset,
        "original": event["fields"]["message"]
    }

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

