import datetime
import json
from typing import Any, Generator

import elasticapm
from event import _default_event
from utils import get_bucket_name_from_arn

from share import Config, logger
from storage import CommonStorage, StorageFactory


def _handle_sqs_event(config: Config, event) -> Generator[tuple[dict[str, Any], int, int, int, int], None, None]:
    for sqs_record_n, sqs_record in enumerate(event["Records"]):
        event_input = config.get_input_by_type_and_id("sqs", sqs_record["eventSourceARN"])
        if not event_input:
            return None

        body = json.loads(sqs_record["body"])
        for s3_record_n, s3_record in enumerate(body["Records"]):
            aws_region = s3_record["awsRegion"]
            bucket_arn = s3_record["s3"]["bucket"]["arn"]
            object_key = s3_record["s3"]["object"]["key"]
            starting_range_offset = s3_record["starting_range_offset"] if "starting_range_offset" in s3_record else 0
            last_decorator_offset = s3_record["last_decorator_offset"] if "last_decorator_offset" in s3_record else 0

            if len(bucket_arn) == 0 or len(object_key) == 0:
                raise Exception("Cannot find bucket_arn or object_key for s3")

            bucket_name: str = get_bucket_name_from_arn(bucket_arn)
            storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name=bucket_name, object_key=object_key
            )

            span = elasticapm.capture_span(f"WAIT FOR OFFSET STARTING AT {starting_range_offset}")
            span.__enter__()
            for log_event, range_offset, decorator_offset in storage.get_by_lines(
                range_start=starting_range_offset, last_decorator_offset=last_decorator_offset
            ):
                # let's be sure that on the first yield `range_offset`
                # doesn't overlap `starting_range_offset`: in case we
                # skip in order to not ingest twice the same event
                if range_offset < starting_range_offset:
                    logger.debug(
                        "skipping event",
                        extra={
                            "range_offset": range_offset,
                            "starting_range_offset": starting_range_offset,
                        },
                    )
                    continue

                if span:
                    span.__exit__(None, None, None)
                    span = None

                es_event = _default_event.copy()
                es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                es_event["fields"]["message"] = log_event.decode("UTF-8")
                es_event["fields"]["log"]["offset"] = decorator_offset

                es_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{1}".format(
                    bucket_name, object_key
                )

                es_event["fields"]["aws"]["s3"] = {
                    "bucket": {"name": bucket_name, "arn": bucket_arn},
                    "object": {"key": object_key},
                }

                es_event["fields"]["cloud"]["region"] = aws_region

                yield es_event, range_offset, decorator_offset, sqs_record_n, s3_record_n
