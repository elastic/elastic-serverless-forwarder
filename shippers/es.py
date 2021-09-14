from typing import Any

import elasticapm  # noqa: F401
from elasticsearch import Elasticsearch

from .shipper import CommonShipper


class ElasticsearchShipper(CommonShipper):
    def __init__(self, hosts: list[str], username: str, password: str, scheme: str, dataset: str, namespace: str):
        self._es_client = Elasticsearch(
            hosts,
            http_auth=(username, password),
            scheme=scheme,
        )

        self._es_index = f"logs-{dataset}-{namespace}"

    def send(self, event: dict[str, Any]) -> Any:
        return self._es_client.index(index=self._es_index, body=event)
