import os

from sqs_trigger import _handle_sqs_event
from utils import (_enrich_event, _from_s3_uri_to_bucket_name_and_object_key,
                   _get_trigger_type)

from share import Config, Target, TargetElasticSearch, parse_config
from shippers import CommonShipper, ShipperFactory
from storage import CommonStorage, StorageFactory

_event_type = "logs"


def lambda_handler(lambda_event, lambda_context):
    try:
        config_file: str = os.getenv("S3_CONFIG_FILE")
        if config_file is None:
            return "empty S3_CONFIG_FILE env variable"

        bucket_name, object_key = _from_s3_uri_to_bucket_name_and_object_key(config_file)

        config_storage: CommonStorage = StorageFactory.create(
            storage_type="s3", bucket_name=bucket_name, object_key=object_key
        )

        c = config_storage.get_as_string()

        config: Config = parse_config(c)
    except Exception as e:
        raise e

    try:
        trigger_type: str = _get_trigger_type(lambda_event)

    except Exception as e:
        raise e

    if trigger_type == "sqs":
        source = config.get_source_by_type_and_name("sqs", lambda_event["Records"][0]["eventSourceARN"])
        if not source:
            return "not source set"

        try:
            shippers: list[CommonShipper] = []
            targets: list[Target] = []
            for target_type in source.get_target_types():
                if target_type == "elasticsearch":
                    target: TargetElasticSearch = source.get_target_by_type("elasticsearch")
                    shipper: CommonShipper = ShipperFactory.create(
                        target="elasticsearch",
                        hosts=target.hosts,
                        scheme=target.scheme,
                        username=target.username,
                        password=target.password,
                        dataset=target.dataset,
                        namespace=target.namespace,
                    )

                    shippers.append(shipper)
                    targets.append(target)

            for es_event, offset in _handle_sqs_event(config, lambda_event):
                for (i, shipper) in enumerate(shippers):
                    target = targets[i]
                    _enrich_event(es_event, _event_type, target.dataset, target.namespace)

                    shipper.send(es_event)

        except Exception as e:
            raise e
