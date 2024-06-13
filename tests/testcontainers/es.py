# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import ssl
import time
from typing import Any

from elasticsearch import Elasticsearch
from OpenSSL import crypto as OpenSSLCrypto
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

DEFAULT_USERNAME = "elastic"
DEFAULT_PASSWORD = "password"


class ElasticsearchContainer(DockerContainer):  # type: ignore
    """
    Elasticsearch container.

    Example
    -------
    ::

        with ElasticsearchContainer() as esc:
            # NOTE: container will terminate once out of this with statement
            url = esc.get_url()
    """

    _DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch"
    _DEFAULT_VERSION = "7.17.20"
    _DEFAULT_PORT = 9200
    _DEFAULT_USERNAME = DEFAULT_USERNAME
    _DEFAULT_PASSWORD = DEFAULT_PASSWORD

    def __init__(
        self,
        image: str = _DEFAULT_IMAGE,
        version: str = _DEFAULT_VERSION,
        port: int = _DEFAULT_PORT,
        username: str = _DEFAULT_USERNAME,
        password: str = _DEFAULT_PASSWORD,
    ):
        image = f"{image}:{version}"
        super(ElasticsearchContainer, self).__init__(image=image, entrypoint="sleep")
        self.with_command("infinity")

        self.port = port
        self.host = ""
        self.exposed_port = 0
        self.ssl_assert_fingerprint = ""

        self.elastic_user: str = username
        self.elastic_password: str = password

        self.with_exposed_ports(self.port)

        self._pipelines_ids: set[str] = set()
        self._index_indices: set[str] = set()

    def _configure(self) -> None:
        """
        Values set here will override any value set by calling <instance>.with_env(...)
        after initializing this class before <instance>.start()
        """

        exit_code, _ = self.get_wrapped_container().exec_run(
            cmd="elasticsearch-certutil cert --silent --name localhost --dns localhost --keep-ca-key "
            "--out /usr/share/elasticsearch/elasticsearch-ssl-http.zip --self-signed --ca-pass '' --pass ''"
        )
        assert exit_code == 0

        exit_code, _ = self.get_wrapped_container().exec_run(
            cmd="unzip /usr/share/elasticsearch/elasticsearch-ssl-http.zip -d /usr/share/elasticsearch/config/certs/"
        )

        assert exit_code == 0

        self.get_wrapped_container().exec_run(
            cmd="/bin/tini -- /usr/local/bin/docker-entrypoint.sh",
            detach=True,
            environment={
                "ES_JAVA_OPTS": "-Xms1g -Xmx1g",
                "ELASTIC_PASSWORD": self.elastic_password,
                "xpack.security.enabled": "true",
                "discovery.type": "single-node",
                "network.bind_host": "0.0.0.0",
                "network.publish_host": "0.0.0.0",
                "logger.org.elasticsearch": "DEBUG",
                "xpack.security.http.ssl.enabled": "true",
                "xpack.security.http.ssl.keystore.path": "/usr/share/elasticsearch/config/certs/localhost/"
                "localhost.p12",
            },
        )

    def get_url(self) -> str:
        return f"https://{self.host}:{self.exposed_port}"

    @wait_container_is_ready()  # type: ignore
    def _connect(self) -> None:
        self.host = self.get_container_host_ip()
        self.exposed_port = int(self.get_exposed_port(self.port))

        while True:
            try:
                pem_server_certificate: str = ssl.get_server_certificate((self.host, self.exposed_port))
                openssl_certificate = OpenSSLCrypto.load_certificate(
                    OpenSSLCrypto.FILETYPE_PEM, pem_server_certificate.encode("utf-8")
                )
            except Exception:
                time.sleep(1)
            else:
                self.ssl_assert_fingerprint = str(openssl_certificate.digest("sha256").decode())
                break

        assert len(self.ssl_assert_fingerprint) > 0

        self.es_client = Elasticsearch(
            hosts=[f"{self.host}:{self.exposed_port}"],
            scheme="https",
            http_auth=(self.elastic_user, self.elastic_password),
            ssl_assert_fingerprint=self.ssl_assert_fingerprint,
            verify_certs=False,
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
            raise_on_error=False,
            raise_on_exception=False,
        )

        while not self.es_client.ping():
            time.sleep(1)

        while True:
            cluster_health = self.es_client.cluster.health(wait_for_status="green")
            if "status" in cluster_health and cluster_health["status"] == "green":
                break

            time.sleep(1)

    def reset(self) -> None:
        for index in self._index_indices:
            self.es_client.indices.delete_data_stream(name=index)

        if self.es_client.indices.exists(index="logs-stash.elasticsearch-output"):
            self.es_client.indices.delete_data_stream(name="logs-stash.elasticsearch-output")

        self._index_indices = set()

        for pipeline_id in self._pipelines_ids:
            self.es_client.ingest.delete_pipeline(id=pipeline_id)

        self._pipelines_ids = set()

    def start(self) -> ElasticsearchContainer:
        super().start()
        self._configure()
        self._connect()
        return self

    def count(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs and ("ignore_unavailable" not in kwargs or kwargs["ignore_unavailable"] is not True):
            self._index_indices.add(kwargs["index"])

        return self.es_client.count(**kwargs)

    def refresh(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs and ("ignore_unavailable" not in kwargs or kwargs["ignore_unavailable"] is not True):
            self._index_indices.add(kwargs["index"])

        return self.es_client.indices.refresh(**kwargs)

    def put_pipeline(self, **kwargs: Any) -> dict[str, Any]:
        if "id" in kwargs:
            self._pipelines_ids.add(kwargs["id"])

        return self.es_client.ingest.put_pipeline(**kwargs)

    def delete_by_query(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs:
            self._index_indices.add(kwargs["index"])

        return self.es_client.delete_by_query(**kwargs)

    def put_settings(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs:
            self._index_indices.add(kwargs["index"])

        return self.es_client.indices.put_settings(**kwargs)

    def exists(self, **kwargs: Any) -> bool:
        exists = self.es_client.indices.exists(**kwargs)
        if exists and "index" in kwargs:
            self._index_indices.add(kwargs["index"])

        return exists

    def search(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs:
            self._index_indices.add(kwargs["index"])

        return self.es_client.search(**kwargs)

    def index(self, **kwargs: Any) -> dict[str, Any]:
        if "index" in kwargs:
            self._index_indices.add(kwargs["index"])

        return self.es_client.index(**kwargs)

    def create_data_stream(self, **kwargs: Any) -> dict[str, Any]:
        if "name" in kwargs:
            self._index_indices.add(kwargs["name"])

        return self.es_client.indices.create_data_stream(**kwargs)
