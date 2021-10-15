# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import json
from copy import deepcopy
from typing import Any, Iterator

import boto3
import elasticapm
from event import _default_event
from utils import get_bucket_name_from_arn

from share import Config, shared_logger
from storage import CommonStorage, StorageFactory


def _handle_sqs_continuation(
    sqs_continuing_queue: str,
    lambda_event: dict[str, Any],
    event_input_id: str,
    last_ending_offset: int,
    current_sqs_record: int,
    current_s3_record: int,
    config_yaml: str,
) -> None:

    sqs_records = lambda_event["Records"][current_sqs_record:]
    body = json.loads(sqs_records[0]["body"])
    body["Records"] = body["Records"][current_s3_record:]
    body["Records"][0]["last_ending_offset"] = last_ending_offset
    sqs_records[0]["body"] = json.dumps(body)

    sqs_client = boto3.client("sqs")
    for sqs_record in sqs_records:
        sqs_client.send_message(
            QueueUrl=sqs_continuing_queue,
            MessageBody=sqs_record["body"],
            MessageAttributes={
                "config": {"StringValue": config_yaml, "DataType": "String"},
                "originalEventSource": {"StringValue": event_input_id, "DataType": "String"},
            },
        )

        shared_logger.debug(
            "continuing", extra={"sqs_continuing_queue": sqs_continuing_queue, "body": sqs_record["body"]}
        )


def _handle_sqs_event(config: Config, event: dict[str, Any]) -> Iterator[tuple[dict[str, Any], int, int, int]]:
    for sqs_record_n, sqs_record in enumerate(event["Records"]):
        event_input = config.get_input_by_type_and_id("sqs", sqs_record["eventSourceARN"])
        if not event_input:
            return None

        body = json.loads(sqs_record["body"])
        for s3_record_n, s3_record in enumerate(body["Records"]):
            aws_region = s3_record["awsRegion"]
            bucket_arn = s3_record["s3"]["bucket"]["arn"]
            object_key = s3_record["s3"]["object"]["key"]
            last_ending_offset = s3_record["last_ending_offset"] if "last_ending_offset" in s3_record else 0

            if len(bucket_arn) == 0 or len(object_key) == 0:
                raise Exception("Cannot find bucket_arn or object_key for s3")

            bucket_name: str = get_bucket_name_from_arn(bucket_arn)
            storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name=bucket_name, object_key=object_key
            )

            shared_logger.info(
                "sqs event",
                extra={
                    "range_start": last_ending_offset,
                    "bucket_arn": bucket_arn,
                    "object_key": object_key,
                },
            )

            span = elasticapm.capture_span(f"WAIT FOR OFFSET STARTING AT {last_ending_offset}")
            span.__enter__()
            events = storage.get_by_lines(
                range_start=last_ending_offset,
            )
            for log_event, ending_offset in events:
                # let's be sure that on the first yield `ending_offset`
                # doesn't overlap `last_ending_offset`: in case we
                # skip in order to not ingest twice the same event
                if ending_offset < last_ending_offset:
                    shared_logger.debug(
                        "skipping event",
                        extra={
                            "ending_offset": ending_offset,
                            "last_ending_offset": last_ending_offset,
                        },
                    )
                    continue

                if span:
                    span.__exit__(None, None, None)
                    span = None

                es_event = deepcopy(_default_event)
                es_event["@timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                es_event["fields"]["message"] = log_event.decode("UTF-8")
                es_event["fields"]["log"]["offset"] = ending_offset - len(log_event)

                es_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{1}".format(
                    bucket_name, object_key
                )

                es_event["fields"]["aws"]["s3"] = {
                    "bucket": {"name": bucket_name, "arn": bucket_arn},
                    "object": {"key": object_key},
                }

                es_event["fields"]["cloud"]["region"] = aws_region

                yield es_event, ending_offset, sqs_record_n, s3_record_n
