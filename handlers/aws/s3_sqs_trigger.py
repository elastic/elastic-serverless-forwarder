# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import json
from copy import deepcopy
from typing import Any, Iterator, Optional
from urllib.parse import unquote_plus

import elasticapm
from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, shared_logger
from storage import CommonStorage, StorageFactory

from .event import _default_event
from .utils import get_account_id_from_arn, get_bucket_name_from_arn


def _handle_s3_sqs_continuation(
    sqs_client: BotoBaseClient,
    sqs_continuing_queue: str,
    last_ending_offset: Optional[int],
    sqs_record: dict[str, Any],
    current_s3_record: int,
    event_input_id: str,
    config_yaml: str,
) -> None:
    """
    Handler of the continuation queue for s3-sqs inputs
    If a sqs message cannot be fully processed before the
    timeout of the lambda this handler will be called: it will
    send new sqs messages for the unprocessed records to the
    internal continuing sqs queue
    """

    body = json.loads(sqs_record["body"])
    body["Records"] = body["Records"][current_s3_record:]
    if last_ending_offset is not None:
        body["Records"][0]["last_ending_offset"] = last_ending_offset

    sqs_record["body"] = json.dumps(body)

    sqs_client.send_message(
        QueueUrl=sqs_continuing_queue,
        MessageBody=sqs_record["body"],
        MessageAttributes={
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalEventSourceARN": {"StringValue": event_input_id, "DataType": "String"},
        },
    )

    shared_logger.debug("continuing", extra={"sqs_continuing_queue": sqs_continuing_queue, "body": sqs_record["body"]})


def _handle_s3_sqs_event(
    sqs_record: dict[str, Any], input_id: str, expand_event_list_from_field: ExpandEventListFromField
) -> Iterator[tuple[dict[str, Any], int, int, bool]]:
    """
    Handler for s3-sqs input.
    It takes an sqs record in the sqs trigger and process
    corresponding object in S3 buckets sending to the defined outputs.
    """

    account_id = get_account_id_from_arn(input_id)

    body = json.loads(sqs_record["body"])
    for s3_record_n, s3_record in enumerate(body["Records"]):
        aws_region = s3_record["awsRegion"]
        bucket_arn = unquote_plus(s3_record["s3"]["bucket"]["arn"], "utf-8")
        object_key = unquote_plus(s3_record["s3"]["object"]["key"], "utf-8")
        last_ending_offset = s3_record["last_ending_offset"] if "last_ending_offset" in s3_record else 0

        assert len(bucket_arn) > 0
        assert len(object_key) > 0

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

        for log_event, json_object, ending_offset, starting_offset, newline_length in events:
            assert isinstance(log_event, bytes)
            if span:
                span.__exit__(None, None, None)
                span = None

            for (
                expanded_log_event,
                expanded_starting_offset,
                is_last_event_expanded,
            ) in expand_event_list_from_field.expand(log_event, json_object, starting_offset, ending_offset):
                es_event = deepcopy(_default_event)
                es_event["@timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                es_event["fields"]["message"] = expanded_log_event.decode("UTF-8")

                es_event["fields"]["log"]["offset"] = expanded_starting_offset

                es_event["fields"]["log"]["file"]["path"] = "https://{0}.s3.{1}.amazonaws.com/{2}".format(
                    bucket_name, aws_region, object_key
                )

                es_event["fields"]["aws"] = {
                    "s3": {
                        "bucket": {"name": bucket_name, "arn": bucket_arn},
                        "object": {"key": object_key},
                    }
                }

                es_event["fields"]["cloud"]["region"] = aws_region
                es_event["fields"]["cloud"]["account"] = {"id": account_id}

                yield es_event, ending_offset, s3_record_n, is_last_event_expanded
