from elasticsearch import Elasticsearch
from shippers.shipper import CommonShipper


class ElasticsearchShipper(CommonShipper):
    def __init__(self, hosts: list[str], username: str, password: str, scheme: str, index: str) -> object:
        self._es_client = Elasticsearch(
            hosts,
            http_auth=(username, password),
            scheme=scheme,
        )

        self._es_index: str = index

    def send(self, event: dict[str, any]) -> any:
        return self._es_client.index(index=self._es_index, body=event)
