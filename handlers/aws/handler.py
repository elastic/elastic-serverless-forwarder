import os

from sqs_trigger import _handle_sqs_event
from utils import _enrich_event, _get_trigger_type

from shippers import ShipperFactory


def lambda_handler(event, context):
    index: str = os.getenv("ES_INDEX")
    shipper: ShipperFactory = ShipperFactory(
        target="elasticsearch",
        hosts=os.getenv("ES_HOSTS").split(","),
        username=os.getenv("ES_USERNAME"),
        password=os.getenv("ES_PASSWORD"),
        scheme=os.getenv("ES_SCHEME"),
        index=index,
    )

    try:
        trigger_type: str = _get_trigger_type(event)

    except Exception as e:
        return str(e)

    event_type, dataset, namespace = index.split("-")

    if trigger_type == "sqs":
        try:
            for es_event, offset in _handle_sqs_event(event):
                _enrich_event(es_event, event_type, dataset, namespace)
                shipper.send(es_event)

        except Exception as e:
            return str(e)
