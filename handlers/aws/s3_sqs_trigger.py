# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any, Iterator, Optional, Union
from urllib.parse import unquote_plus

import elasticapm
from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, json_dumper, json_parser, shared_logger
from storage import ProtocolStorage, StorageFactory

from .utils import (
    discover_integration_scope,
    expand_event_list_from_field_resolver,
    get_account_id_from_arn,
    get_bucket_name_from_arn,
)


def _handle_s3_sqs_move(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    sqs_record: dict[str, Any],
    input_id: str,
    config_yaml: str,
    current_s3_record: int = 0,
    continuing_queue: bool = True,
    last_ending_offset: Optional[int] = None,
    last_event_expanded_offset: Optional[int] = None,
) -> None:
    """
    Handler of the continuation/replay queue for s3-sqs inputs.
    If a sqs message cannot be fully processed before the timeout of the lambda, the handler will be called
    for the continuation queue: it will send new sqs messages for the unprocessed records to the
    internal continuing sqs queue.
    If a sqs message has an eventSourceARN not present in the config.yaml ids, then the handler should be called,
    so it can get placed in the internal replay queue.

    :param continuing_queue: should be set to true if the sqs message is going to be placed in the continuing
    queue. Otherwise, we assume it will be placed in the replaying queue, and, in that case, it should be set to false.
    """

    body = json_parser(sqs_record["body"])
    body["Records"] = body["Records"][current_s3_record:]
    if last_ending_offset is not None:
        body["Records"][0]["last_ending_offset"] = last_ending_offset

    if last_event_expanded_offset is not None:
        body["Records"][0]["last_event_expanded_offset"] = last_event_expanded_offset
    elif "last_event_expanded_offset" in body["Records"][0]:
        del body["Records"][0]["last_event_expanded_offset"]

    sqs_record["body"] = json_dumper(body)

    sqs_client.send_message(
        QueueUrl=sqs_destination_queue,
        MessageBody=sqs_record["body"],
        MessageAttributes={
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalEventSourceARN": {"StringValue": input_id, "DataType": "String"},
        },
    )

    if continuing_queue:
        shared_logger.debug(
            "continuing",
            extra={
                "sqs_continuing_queue": sqs_destination_queue,
                "last_ending_offset": last_ending_offset,
                "last_event_expanded_offset": last_event_expanded_offset,
                "current_s3_record": current_s3_record,
            },
        )
    else:
        shared_logger.debug(
            "replaying",
            extra={
                "sqs_replaying_queue": sqs_destination_queue,
                "input_id": input_id,
                "message_id": sqs_record["messageId"],
            },
        )


def _handle_s3_sqs_event(
    sqs_record_body: dict[str, Any],
    input_id: str,
    field_to_expand_event_list_from: str,
    root_fields_to_add_to_expanded_event: Optional[Union[str, list[str]]],
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[dict[str, Any], int, Optional[int], int]]:
    """
    Handler for s3-sqs input.
    It takes an sqs record in the sqs trigger and process
    corresponding object in S3 buckets sending to the defined outputs.
    """

    account_id = get_account_id_from_arn(input_id)

    for s3_record_n, s3_record in enumerate(sqs_record_body["Records"]):
        aws_region = s3_record["awsRegion"]
        bucket_arn = unquote_plus(s3_record["s3"]["bucket"]["arn"], "utf-8")
        object_key = unquote_plus(s3_record["s3"]["object"]["key"], "utf-8")
        event_time = int(datetime.datetime.strptime(s3_record["eventTime"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
        last_ending_offset = s3_record["last_ending_offset"] if "last_ending_offset" in s3_record else 0
        last_event_expanded_offset = (
            s3_record["last_event_expanded_offset"] if "last_event_expanded_offset" in s3_record else None
        )

        integration_scope = discover_integration_scope(object_key)

        event_list_from_field_expander = ExpandEventListFromField(
            field_to_expand_event_list_from,
            integration_scope,
            expand_event_list_from_field_resolver,
            root_fields_to_add_to_expanded_event,
            last_event_expanded_offset,
        )

        assert len(bucket_arn) > 0
        assert len(object_key) > 0

        bucket_name: str = get_bucket_name_from_arn(bucket_arn)
        storage: ProtocolStorage = StorageFactory.create(
            storage_type="s3",
            bucket_name=bucket_name,
            object_key=object_key,
            json_content_type=json_content_type,
            event_list_from_field_expander=event_list_from_field_expander,
            multiline_processor=multiline_processor,
        )

        span = elasticapm.capture_span(f"WAIT FOR OFFSET STARTING AT {last_ending_offset}")
        span.__enter__()
        events = storage.get_by_lines(range_start=last_ending_offset)

        for log_event, starting_offset, ending_offset, event_expanded_offset in events:
            assert isinstance(log_event, bytes)

            if span:
                span.__exit__(None, None, None)
                span = None

            es_event: dict[str, Any] = {
                "@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "fields": {
                    "message": log_event.decode("utf-8"),
                    "log": {
                        "offset": starting_offset,
                        "file": {
                            "path": "https://{0}.s3.{1}.amazonaws.com/{2}".format(bucket_name, aws_region, object_key),
                        },
                    },
                    "aws": {
                        "s3": {
                            "bucket": {"name": bucket_name, "arn": bucket_arn},
                            "object": {"key": object_key},
                        }
                    },
                    "cloud": {
                        "provider": "aws",
                        "region": aws_region,
                        "account": {"id": account_id},
                    },
                },
                "meta": {"event_time": event_time, "integration_scope": integration_scope},
            }

            yield es_event, ending_offset, event_expanded_offset, s3_record_n
