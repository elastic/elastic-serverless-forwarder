import os
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
import json
import time

from typing import Optional

class LogstashContainer(DockerContainer):
    """
    Logstash container.

    Example
    -------
    ::

        with LogstashContainer() as lc:
            url = lc.get_url()
    """
    _DEFAULT_IMAGE = "docker.elastic.co/logstash/logstash"
    _DEFAULT_VERSION = "8.5.0"
    _DEFAULT_PORT = 5044
    _DEFAULT_API_PORT = 9600

    def __init__(self, image:str=_DEFAULT_IMAGE, version:str=_DEFAULT_VERSION, port:int=_DEFAULT_PORT, api_port:int=_DEFAULT_API_PORT):
        image = f"{image}:{version}"
        super(LogstashContainer, self).__init__(image=image)

        self.port = port
        self.api_port = api_port

        self.with_exposed_ports(self.port)
        self.with_exposed_ports(self.api_port)
        self._configure()

    def _configure(self):
        """
        You can override any value set here by calling <instance>.with_env(...) after initializing this class
        """
        self.with_env("LOG_LEVEL", os.environ.get("LOGSTASH_LOG_LEVEL", "warn"))
        self.with_env("CONFIG_STRING", "input { stdin {} } output { stdout {} }")
        self.with_env("XPACK_MONITORING_ENABLED", "false")


    def get_url(self):
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        return "http://{}:{}".format(host, port)

    def get_apiurl(self):
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.api_port)
        return "http://{}:{}".format(host, port)

    @wait_container_is_ready(requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout)
    def _connect(self):
        url = self.get_apiurl()
        response = requests.get("{}/?pretty".format(url), timeout=1)
        response.raise_for_status()

    def start(self):
        super().start()
        self._connect()
        return self

    def get_messages(self, retry:int=2, timeout:int=1) -> list[dict]:
        """
        Extract JSON messages from stdout as printed by Logstash with codec json or json_lines.

        NOTE: to achieve the desired result the Logstash configuration should output
        JSON to stdout using stdout plugin with codec json or json_lines.

        :param retry: The number of times to retry fetch messages.
            To disable set it to 0.
        :param timeout: The timeout in seconds to wait between each retry.
            To disable set it to 0.
        :return: The list of parsed messages.
        """
        stdout, stderr = self.get_logs()
        lines = stdout.decode().split("\n")
        messages = []
        for l in lines:
            try:
                msg = json.loads(l)
                messages.append(msg)
            except json.decoder.JSONDecodeError:
                # NOTE: if a line is not valid JSON is not a message sent to stdout,
                # so we can safely ignore it
                pass

        # NOTE: a delay has been observed between data beign sent to Logstash and it 
        # being available in the Docker stdout. To accout for this delay without 
        # the need to add sleeps in other areas leverage this retry logic (enabled
        # by default)
        if len(messages) == 0 and retry > 0:
            if timeout > 0:
                time.sleep(timeout)
            return self.get_messages(retry-1, timeout)

        return messages
