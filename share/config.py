from abc import ABCMeta, abstractmethod
from typing import Optional

import yaml

_available_source_type: list[str] = ["sqs"]
_available_target_type: list[str] = ["elasticsearch"]


def _raise(e: Exception):
    raise e


class Target:
    __metaclass__ = ABCMeta

    def __init__(self, target_type: str, kwargs: dict[str, any]):
        self.type: str = target_type
        self.kwargs: dict[str, any] = kwargs

    @property
    @abstractmethod
    def kwargs(self) -> None:
        pass

    @kwargs.setter
    @abstractmethod
    def kwargs(self, value: dict[str, any]):
        pass

    @property
    def type(self) -> Optional[str]:
        return self._type

    @type.setter
    def type(self, value):
        if not isinstance(value, str):
            raise ValueError("Target type must by of type str")

        if value not in _available_target_type:
            raise ValueError(f"Type must be one of {','.join(_available_target_type)}")
        self._type = value


class TargetElasticSearch(Target):
    _kwargs = ["hosts", "scheme", "username", "password", "dataset", "namespace"]

    def __init__(self, target_type: str, kwargs: dict[str, any]):
        self._hosts: Optional[list[str]] = None
        self._scheme: Optional[str] = None
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._dataset: Optional[str] = None
        self._namespace: Optional[str] = None

        super().__init__(target_type, kwargs)

    @property
    def kwargs(self) -> None:
        return None

    @kwargs.setter
    def kwargs(self, value: dict[str, any]):
        [self.__setattr__(x, value[x]) for x in value.keys() if x in self._kwargs]

        [
            _raise(ValueError(f"Empty param {x} provided for Target Elasticsearch"))
            for x in self._kwargs
            if self.__getattribute__(x) is None
        ]

    @property
    def hosts(self) -> Optional[list[str]]:
        return self._hosts

    @hosts.setter
    def hosts(self, value):
        if not isinstance(value, list):
            raise ValueError("Elasticsearch Target hosts must by of type list[str]")

        self._hosts = value

    @property
    def scheme(self) -> Optional[str]:
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Target scheme must by of type str")

        self._scheme = value

    @property
    def username(self) -> Optional[str]:
        return self._username

    @username.setter
    def username(self, value):
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Target username must by of type str")

        self._username = value

    @property
    def password(self) -> Optional[str]:
        return self._password

    @password.setter
    def password(self, value):
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Target password must by of type str")

        self._password = value

    @property
    def dataset(self) -> Optional[str]:
        return self._dataset

    @dataset.setter
    def dataset(self, value):
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Target dataset must by of type str")

        self._dataset = value

    @property
    def namespace(self) -> Optional[str]:
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Target namespace must by of type str")

        self._namespace = value


class Source:
    def __init__(self, source_type: str, source_name: str):
        self.type = source_type
        self.name = source_name
        self._targets: dict[str, Target] = {}

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        if not isinstance(value, str):
            raise ValueError("Source type must by of type str")

        if value not in _available_source_type:
            raise ValueError(f"Type must be one of {','.join(_available_source_type)}")
        self._type = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if not isinstance(value, str):
            raise ValueError("Source type must by of type str")
        self._name = value

    def get_target_by_type(self, target_type: str) -> Optional[Target]:
        return self._targets[target_type] if target_type in self._targets else None

    def get_target_types(self) -> list[str]:
        return self._targets.keys()

    def add_target(self, target_type: str, target_kwargs: dict[str, any]):
        if not isinstance(target_type, str):
            raise ValueError("Target type must by of type str")

        if not isinstance(target_kwargs, dict):
            raise ValueError("Target arguments must by of type dict[str,any]")

        if target_type in self._targets:
            raise ValueError("duplicated target {target_type}")

        target: Optional[Target] = None
        if target_type == "elasticsearch":
            target = TargetElasticSearch(target_type=target_type, kwargs=target_kwargs)

        self._targets[target.type] = target


class Config:
    def __init__(self):
        self._sources = {}

    def get_source_by_type_and_name(self, source_type: str, source_name: str) -> Optional[Source]:
        if source_type not in self._sources:
            return None

        return self._sources[source_type][source_name] if source_name in self._sources[source_type] else None

    def add_source(self, source: Source):
        if source.type not in self._sources:
            self._sources[source.type] = {source.name: source}

            return

        if source.name in self._sources[source.type]:
            raise ValueError(f"duplicated source {source.type}/{source.name}")

        self._sources[source.type][source.name] = source


def parse_config(config_yaml: str) -> Config:
    yaml_config = yaml.safe_load(config_yaml)

    conf: Config = Config()

    if "sources" not in yaml_config or not isinstance(yaml_config["sources"], list):
        raise ValueError("no sources provided")

    for source_config in yaml_config["sources"]:
        if "type" not in source_config:
            raise ValueError("Must by provided type for target")

        if "name" not in source_config:
            raise ValueError("Must by provided type for name")

        source: Source = Source(source_type=source_config["type"], source_name=source_config["name"])

        if "targets" not in source_config or not isinstance(source_config["targets"], list):
            raise ValueError("No valid targets for source")

        for target_config in source_config["targets"]:
            if "type" not in target_config:
                raise ValueError("Must by provided type for target")

            if "args" not in target_config:
                raise ValueError("Must by provided args for target")

            source.add_target(target_type=target_config["type"], target_kwargs=target_config["args"])

        conf.add_source(source)

    return conf
