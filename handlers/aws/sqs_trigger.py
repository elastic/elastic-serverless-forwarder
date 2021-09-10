import datetime
import json
from typing import Generator

from event import _default_event
from utils import _get_bucket_name_from_arn

from share import Config
from storage import CommonStorage, StorageFactory


def _handle_sqs_event(config: Config, event) -> Generator[tuple[dict[str, any], int], None, None]:
    for sqs_record in event["Records"]:
        source = config.get_source_by_type_and_name("sqs", sqs_record["eventSourceARN"])
        if not source:
            return None

        body = json.loads(sqs_record["body"])
        for s3_record in body["Records"]:
            aws_region = s3_record["awsRegion"]
            bucket_arn = s3_record["s3"]["bucket"]["arn"]
            object_key = s3_record["s3"]["object"]["key"]

            if len(bucket_arn) == 0 or len(object_key) == 0:
                raise Exception("cannot find bucket_arn or object_key for s3")

            bucket_name: str = _get_bucket_name_from_arn(bucket_arn)
            storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name=bucket_name, object_key=object_key
            )

            for log_event, offset in storage.get_by_lines():
                es_event = _default_event.copy()
                es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                es_event["fields"]["message"] = log_event.decode("UTF-8")
                es_event["fields"]["log"]["offset"] = offset

                es_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{1}".format(
                    bucket_name, object_key
                )

                es_event["fields"]["aws"]["s3"] = {
                    "bucket": {"name": bucket_name, "arn": bucket_arn},
                    "object": {"key": object_key},
                }

                es_event["fields"]["cloud"]["region"] = aws_region

                yield es_event, offset
