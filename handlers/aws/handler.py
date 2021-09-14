import json
import os

import boto3
import elasticapm  # noqa: F401
from elasticapm.contrib.serverless.aws import capture_serverless  # noqa: F401
from sqs_trigger import _handle_sqs_event
from utils import from_s3_uri_to_bucket_name_and_object_key, get_trigger_type

from share import Config, ElasticSearchOutput, Output, get_logger, parse_config
from shippers import CommonShipper, ShipperFactory
from storage import CommonStorage, StorageFactory

_event_type: str = "logs"
_completion_grace_period: int = 9000000000

logger = get_logger("aws.handler")


@capture_serverless()
def lambda_handler(lambda_event, lambda_context):
    try:
        trigger_type: str = get_trigger_type(lambda_event)
        logger.info("trigger", extra={"type": trigger_type})
    except Exception as e:
        logger.exception("exception raised", exc_info=e)
        return str(e)

    try:
        if trigger_type == "self_sqs":
            payload = lambda_event["Records"][0]["messageAttributes"]
            config_yaml = payload["config"]["stringValue"]
            config: Config = parse_config(config_yaml)
            lambda_event["Records"][0]["eventSourceARN"] = payload["originalEventSource"]["stringValue"]

        else:
            config_file: str = os.getenv("S3_CONFIG_FILE")
            if config_file is None:
                return "empty S3_CONFIG_FILE env variable"

            bucket_name, object_key = from_s3_uri_to_bucket_name_and_object_key(config_file)

            config_storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name=bucket_name, object_key=object_key
            )

            config_yaml = config_storage.get_as_string()
            logger.debug("config", extra={"yaml": config_yaml})

            config: Config = parse_config(config_yaml)

    except Exception as e:
        logger.exception("exception raised", exc_info=e)
        return str(e)

    if trigger_type == "sqs" or trigger_type == "self_sqs":
        event_input = config.get_input_by_type_and_id("sqs", lambda_event["Records"][0]["eventSourceARN"])
        if not event_input:
            logger.error(f'no input set for {lambda_event["Records"][0]["eventSourceARN"]}')

            return "not input set"

        logger.info("input", extra={"type": event_input.type, "id": event_input.id})

        try:
            shippers: list[CommonShipper] = []
            outputs: list[Output] = []

            for output_type in event_input.get_output_types():
                if output_type == "elasticsearch":
                    logger.info("setting ElasticSearch shipper")
                    output: ElasticSearchOutput = event_input.get_output_by_type("elasticsearch")
                    shipper: CommonShipper = ShipperFactory.create(
                        output="elasticsearch",
                        hosts=output.hosts,
                        scheme=output.scheme,
                        username=output.username,
                        password=output.password,
                        dataset=output.dataset,
                        namespace=output.namespace,
                    )

                    shippers.append(shipper)
                    outputs.append(output)

            for es_event, offset, sqs_record_n, s3_record_n in _handle_sqs_event(config, lambda_event):
                for (output_n, shipper) in enumerate(shippers):
                    logger.debug(
                        "processing",
                        extra={
                            "offset": offset,
                            "output_n": output_n,
                            "sqs_record_n": sqs_record_n,
                            "s3_record_n": s3_record_n,
                        },
                    )

                    current_output = outputs[output_n]
                    current_output.enrich_event(es_event, _event_type)
                    logger.debug("es_event", extra={"es_event": es_event})

                    shipper.send(es_event)
                    logger.info("sent to output", extra={"type": current_output.type})

                    if (
                        lambda_context is not None
                        and lambda_context.get_remaining_time_in_millis() < _completion_grace_period
                    ):
                        sqs_continuing_queue = os.environ["SQS_CONTINUE_URL"]

                        logger.info(
                            "lambda is going to shutdown, continuing on dedicated sqs queue",
                            extra={"sqs_queue": sqs_continuing_queue},
                        )

                        # Cleaner rewrite needed
                        sqs_records = lambda_event["Records"][sqs_record_n:]
                        body = json.loads(sqs_records[0]["body"])
                        body["Records"] = body["Records"][s3_record_n:]
                        # move offset forward so that we will start
                        # from the next after the current one
                        body["Records"][0]["starting_offset"] = offset + 1
                        sqs_records[0]["body"] = json.dumps(body)

                        sqs_client = boto3.client("sqs")
                        for sqs_record in sqs_records:
                            sqs_client.send_message(
                                QueueUrl=sqs_continuing_queue,
                                MessageBody=sqs_record["body"],
                                MessageAttributes={
                                    "config": {"StringValue": config_yaml, "DataType": "String"},
                                    "originalEventSource": {"StringValue": event_input.id, "DataType": "String"},
                                },
                            )

                            logger.debug(
                                "continuing", extra={"sqs_continuing_queue": sqs_continuing_queue, "body": body}
                            )

                        return

        except Exception as e:
            logger.exception("exception raised", exc_info=e)
            return str(e)
