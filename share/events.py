# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any


def normalise_event(event_payload: dict[str, Any]) -> dict[str, Any]:
    """
    This method move fields payload in the event at root level and then removes it with meta payload
    It has to be called as last step after any operation on the event payload just before sending to the cluster
    """
    if "fields" in event_payload:
        fields: dict[str, Any] = event_payload["fields"]
        for field_key in fields.keys():
            event_payload[field_key] = fields[field_key]

        del event_payload["fields"]

    if "meta" in event_payload:
        del event_payload["meta"]

    return event_payload
