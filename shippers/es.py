from typing import Any

import elasticapm  # noqa: F401
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk

from .shipper import CommonShipper


class ElasticsearchShipper(CommonShipper):
    _bulk_actions: list[dict, Any] = []
    _bulk_batch_size: int = 1000

    def __init__(self, hosts: list[str], username: str, password: str, scheme: str, dataset: str, namespace: str):
        self._es_client = Elasticsearch(
            hosts,
            http_auth=(username, password),
            scheme=scheme,
        )

        self._dataset = dataset
        self._namespace = namespace

        self._es_index = f"logs-{dataset}-{namespace}"

    def _enrich_event(self, event_payload: dict[str, any]) -> None:
        event_payload["data_stream"] = {
            "type": "logs",
            "dataset": self._dataset,
            "namespace": self._namespace,
        }

        event_payload["event"] = {"dataset": self._dataset, "original": event_payload["fields"]["message"]}

        event_payload["tags"] = ["preserve_original_event", "forwarded", self._dataset.replace(".", "-")]

    def send(self, event: dict[str, Any]) -> Any:
        self._enrich_event(event_payload=event)

        event["_op_type"] = "create"
        event["_index"] = self._es_index
        self._bulk_actions.append(event)

        if len(self._bulk_actions) < self._bulk_batch_size:
            return

        es_bulk(self._es_client, self._bulk_actions)
        self._bulk_actions = []

    def flush(self):
        if len(self._bulk_actions) > 1:
            es_bulk(self._es_client, self._bulk_actions)

        self._bulk_actions = []
