import os

from shippers import ShipperFactory
from sqs_trigger import _handle_sqs_event
from utils import _get_trigger_type, _enrich_event


def handler_name(event, context):
    index: str = os.getenv("INDEX")
    shipper: ShipperFactory = ShipperFactory(target="elasticsearch",
                                             hosts=os.getenv("HOSTS").split(","),
                                             username=os.getenv("USERNAME"),
                                             password=os.getenv("PASSWORD"),
                                             scheme=os.getenv("SCHEME"),
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
