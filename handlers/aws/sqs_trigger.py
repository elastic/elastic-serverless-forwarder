import datetime
from typing import Generator
from storage import StorageFactory
from utils import _get_bucket_name_from_arn
from event import _default_event


def _handle_sqs_event(event) -> Generator[tuple[dict[str, any], int], None, None]:
    bucket_arn: str = ""
    object_key: str = ""

    for record in event["Records"]:
        bucket_arn = record["s3"]["bucket"]["arn"]
        object_key = record["s3"]["object"]["key"]

        if len(bucket_arn) == 0 or len(object_key) == 0:
            raise Exception("cannot find bucket_arn or object_key for s3")

        bucket_name: str = _get_bucket_name_from_arn(bucket_arn)
        storage: StorageFactory = StorageFactory(storage_type="s3", bucket_arn=bucket_arn, object_key=object_key)

        for log_event, offset in storage.get():
            es_event = _default_event.copy()
            es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            es_event["fields"]["message"] = log_event.decode("UTF-8")
            es_event["fields"]["log"]["offset"] = offset

            es_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{1}".format(
                bucket_name,
                object_key
            )

            es_event["fields"]["aws"]["s3"] = {
                "bucket": {"name": bucket_name, "arn": bucket_arn},
                "object": {"key": object_key},
            }

            es_event["fields"]["cloud"]["region"] = ""

            yield es_event, offset
