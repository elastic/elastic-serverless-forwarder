from shippers.common.commonshipper import CommonShipper
from elasticsearch import Elasticsearch


class ElasticsearchShipper(CommonShipper):
    def __init__(self, hosts: list[str], username: str, password: str, scheme: str, index: str) -> object:
        self._es_client = Elasticsearch(
            hosts,
            http_auth=(username, password),
            scheme=scheme,
        )

        self._es_index: str = index

    def send(self, event: dict[str, any]):
        self._es_client.index(index=self._es_index, body=event)
