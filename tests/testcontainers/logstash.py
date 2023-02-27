# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
import ssl
import time
from typing import Any

import requests
from OpenSSL import crypto as OpenSSLCrypto
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_container_is_ready  # type: ignore

from share import json_parser


class LogstashContainer(DockerContainer):  # type: ignore
    """
    Logstash container.

    Example
    -------
    ::

        with LogstashContainer() as lc:
            # NOTE: container will terminate once out of this with statement
            url = lc.get_url()
    """

    _DEFAULT_IMAGE = "docker.elastic.co/logstash/logstash"
    _DEFAULT_VERSION = "7.16.0"
    _DEFAULT_PORT = 5044
    _DEFAULT_API_PORT = 9600
    _DEFAULT_USERNAME = "USERNAME"
    _DEFAULT_PASSWORD = "PASSWORD"

    def __init__(
        self,
        image: str = _DEFAULT_IMAGE,
        version: str = _DEFAULT_VERSION,
        port: int = _DEFAULT_PORT,
        api_port: int = _DEFAULT_API_PORT,
        username: str = _DEFAULT_USERNAME,
        password: str = _DEFAULT_PASSWORD,
    ):
        image = f"{image}:{version}"
        super(LogstashContainer, self).__init__(image=image)

        self.port = port
        self.api_port = api_port
        self.exposed_port = 0
        self.ssl_assert_fingerprint = ""

        self.logstash_user: str = username
        self.logstash_password: str = password

        self.with_exposed_ports(self.port)
        self.with_exposed_ports(self.api_port)

        self._last_reset_message_count: int = 0
        self._previous_message_count: int = 0

    def _configure(self) -> None:
        """
        Values set here will override any value set by calling <instance>.with_env(...)
        after initializing this class before <instance>.start()
        """

        # TODO: let's generate the certificate and key with code, current cert will expire Nov 17 15:04:09 2025 GMT
        local_ssl_path = os.path.join(os.path.dirname(__file__), "ssl")
        self.with_volume_mapping(local_ssl_path, "/ssl")

        self.with_command(
            'bash -c "/opt/logstash/bin/logstash-plugin install '
            'logstash-input-elastic_serverless_forwarder && /opt/logstash/bin/logstash"'
        )

        # NOTE: plain curly brackets must be escaped in this string (double them)
        logstash_config = f"""\
            input {{
              elastic_serverless_forwarder {{
                port => {self.port}
                ssl_certificate => "/ssl/localhost.crt"
                ssl_key => "/ssl/localhost.pkcs8.key"
                auth_basic_username => "{self.logstash_user}"
                auth_basic_password => "{self.logstash_password}"
              }}
            }}
            output {{ stdout {{ codec => json_lines }} }}
            """

        self.with_env("LOG_LEVEL", os.environ.get("LOGSTASH_LOG_LEVEL", "fatal"))
        self.with_env("PIPELINE_WORKERS", "1")
        # By design and by default, Logstash does not guarantee event order. The following setting ensures that
        # events are processed in the order they are received.
        # Ref: https://www.elastic.co/guide/en/logstash/current/processing.html
        self.with_env("PIPELINE_ORDERED", "true")
        self.with_env("MONITORING_ENABLED", "false")
        self.with_env("CONFIG_STRING", logstash_config)

    def get_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        return f"https://{host}:{port}"

    def get_api_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.api_port)
        return f"http://{host}:{port}"

    @wait_container_is_ready(requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout)  # type: ignore
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

        url = self.get_api_url()
        response = requests.get(f"{url}/?pretty", timeout=1)
        response.raise_for_status()

    def reset(self) -> None:
        self._last_reset_message_count = self._previous_message_count

    def start(self) -> "LogstashContainer":
        self._configure()
        super().start()
        self._connect()
        return self

    def get_messages(self, expected: int, retry: int = 2, wait_time: int = 1, delay: int = 1) -> list[dict[str, Any]]:
        """
        Extract N expected JSON messages from stdout as printed by Logstash with codec json or json_lines.

        NOTE: to achieve the desired result the Logstash configuration should output
        JSON to stdout using stdout plugin with codec json or json_lines.

        NOTE: a delay has been observed between data being sent to Logstash and it
        being available in the Docker stdout. To accout for this delay without
        the need to add sleeps in other areas this function has a retry logic (enabled
        by default) implemented. After an initial delay a retry mechanism is triggered
        based on number of messages expected in the docker output, with an adjustable
        timeout between retries.
        :param expected: The number of messages to fetch.
            It accepts a 0 value.
        :param retry: The number of times to retry fetch messages.
            To disable set it to 0.
        :param wait_time: The time in seconds to wait between each retry.
            To disable set it to 0.
        :param delay: The delay to start gathering logs from Docker daemon.
            To disable set it to 0.
        :return: The list of parsed messages.
        """

        time.sleep(delay)
        stdout: bytes = b""
        stdout, _ = self.get_logs()
        messages = []
        lines_count: int = stdout.count(b"\n")
        if lines_count < expected:
            if wait_time > 0:
                time.sleep(wait_time)
            return self.get_messages(expected=expected, retry=retry, wait_time=wait_time, delay=0)

        lines = stdout.decode("utf-8").splitlines()
        for line in lines:
            try:
                msg = json_parser(line)
                messages.append(msg)
            except Exception:
                # NOTE: if a line is not valid JSON is not a message sent to stdout,
                # so we can safely ignore it
                pass

        # Using the previous message count allows subsequent calls to this function
        # to properly trigger replay logic.
        if len(messages) < expected and retry > 0:
            if wait_time > 0:
                time.sleep(wait_time)
            return self.get_messages(expected=expected, retry=retry - 1, wait_time=wait_time, delay=0)

        self._previous_message_count = len(messages)

        return messages[self._last_reset_message_count :]
