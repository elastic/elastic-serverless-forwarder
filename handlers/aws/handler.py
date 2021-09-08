import os

from shippers import ShipperFactory
from sqs_trigger import _handle_sqs_event
from utils import _get_trigger_type


def handler_name(event, context):
    shipper: ShipperFactory = ShipperFactory(target="elasticsearch",
                                             hosts=os.getenv("HOSTS"),
                                             username=os.getenv("USERNAME"),
                                             password=os.getenv("PASSWORD"),
                                             scheme=os.getenv("SCHEME"),
                                             index=os.getenv("INDEX"),
                                             )

    try:
        trigger_type: str = _get_trigger_type(event)

    except Exception as e:
        return str(e)

    if trigger_type == "sqs":
        try:
            for es_event, offset in _handle_sqs_event(event):
                shipper.send(es_event)

        except Exception as e:
            return str(e)

