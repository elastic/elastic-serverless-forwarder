import os
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

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
