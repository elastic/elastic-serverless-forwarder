# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from elasticsearch import Elasticsearch

from .shipper import CommonShipper


class ElasticsearchShipper(CommonShipper):
    def __init__(self, hosts: list[str], username: str, password: str, scheme: str, index: str):
        self._es_client = Elasticsearch(
            hosts,
            http_auth=(username, password),
            scheme=scheme,
        )

        self._es_index: str = index

    def send(self, event: dict[str, any]) -> any:
        return self._es_client.index(index=self._es_index, body=event)
