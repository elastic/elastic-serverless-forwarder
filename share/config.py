# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from abc import ABCMeta, abstractmethod
from typing import Any, Optional

import json
import yaml

_available_input_types: list[str] = ["sqs"]
_available_output_types: list[str] = ["elasticsearch"]


class Output(metaclass=ABCMeta):
    def __init__(self, output_type: str, kwargs: dict[str, Any]):
        self.type: str = output_type
        self.kwargs: dict[str, Any] = kwargs  # type: ignore # (https://github.com/python/mypy/issues/10692)

    @property  # type:ignore # (https://github.com/python/mypy/issues/4165)
    @abstractmethod
    def kwargs(self) -> dict[str, Any]:
        raise NotImplementedError

    @kwargs.setter  # type: ignore # (https://github.com/python/mypy/issues/10692)
    @abstractmethod
    def kwargs(self, value: dict[str, Any]) -> None:
        raise NotImplementedError

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Output type must by of type str")

        if value not in _available_output_types:
            raise ValueError(f"Type must be one of {','.join(_available_output_types)}")
        self._type = value


class ElasticSearchOutput(Output):
    _kwargs = ["hosts", "scheme", "username", "password", "dataset", "namespace"]

    def __init__(self, output_type: str, kwargs: dict[str, Any]):
        self._hosts: list[str] = []
        self._scheme: str = ""
        self._username: str = ""
        self._password: str = ""
        self._dataset: str = ""
        self._namespace: str = ""

        if output_type != "elasticsearch":
            raise ValueError("output_type for ElasticSearchOutput must be elasticsearch")

        super().__init__(output_type, kwargs)

    @property
    def kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}

        for k in self._kwargs:
            v: Any = self.__getattribute__(k)
            if v is not None:
                kwargs[k] = v

        return kwargs

    @kwargs.setter
    def kwargs(self, value: dict[str, Any]) -> None:
        init_kwargs: list[str] = [key for key in value if key in self._kwargs]
        if len(init_kwargs) != len(self._kwargs):
            raise ValueError(
                f"you must provide the following not empty init kwargs for elasticsearch:"
                f" {', '.join(self._kwargs)}. (provided: {json.dumps(value)})"
            )

        for x in value.keys():
            if x in self._kwargs:
                self.__setattr__(x, value[x])
                if not self.__getattribute__(x):
                    raise ValueError(f"Empty param {x} provided for Elasticsearch Output")

    @property
    def hosts(self) -> list[str]:
        return self._hosts

    @hosts.setter
    def hosts(self, value: list[str]) -> None:
        if not isinstance(value, list):
            raise ValueError("Elasticsearch Output hosts must by of type list[str]")

        self._hosts = value

    @property
    def scheme(self) -> str:
        return self._scheme

    @scheme.setter
    def scheme(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output scheme must by of type str")

        self._scheme = value

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output username must by of type str")

        self._username = value

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output password must by of type str")

        self._password = value

    @property
    def dataset(self) -> str:
        return self._dataset

    @dataset.setter
    def dataset(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output dataset must by of type str")

        self._dataset = value

    @property
    def namespace(self) -> str:
        return self._namespace

    @namespace.setter
    def namespace(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output namespace must by of type str")

        self._namespace = value


class Input:
    def __init__(self, input_type: str, input_id: str):
        self.type = input_type
        self.id = input_id
        self._outputs: dict[str, Output] = {}

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Input type must by of type str")

        if value not in _available_input_types:
            raise ValueError(f"Input type must be one of {','.join(_available_input_types)}")
        self._type = value

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Input id must by of type str")
        self._id = value

    def get_output_by_type(self, output_type: str) -> Optional[Output]:
        return self._outputs[output_type] if output_type in self._outputs else None

    def get_output_types(self) -> list[str]:
        return list(self._outputs.keys())

    def delete_output_by_type(self, output_type: str) -> None:
        del self._outputs[output_type]

    def add_output(self, output_type: str, output_kwargs: dict[str, Any]) -> None:
        if not isinstance(output_type, str):
            raise ValueError("Output type must by of type str")

        if not isinstance(output_kwargs, dict):
            raise ValueError("Output arguments must by of type dict[str, Any]")

        if output_type in self._outputs:
            raise ValueError(f"Duplicated Output {output_type}")

        output: Optional[Output] = None
        if output_type == "elasticsearch":
            output = ElasticSearchOutput(output_type=output_type, kwargs=output_kwargs)

        assert output is not None
        self._outputs[output.type] = output


class Config:
    def __init__(self) -> None:
        self._inputs: dict[str, dict[str, Input]] = {}

    def get_input_by_type_and_id(self, input_type: str, input_id: str) -> Optional[Input]:
        if input_type not in self._inputs:
            return None

        return self._inputs[input_type][input_id] if input_id in self._inputs[input_type] else None

    def add_input(self, new_input: Input) -> None:
        if new_input.type not in self._inputs:
            self._inputs[new_input.type] = {new_input.id: new_input}

            return

        if new_input.id in self._inputs[new_input.type]:
            raise ValueError(f"duplicated input {new_input.type}/{new_input.id}")

        self._inputs[new_input.type][new_input.id] = new_input


def parse_config(config_yaml: str) -> Config:
    yaml_config = yaml.safe_load(config_yaml)
    assert isinstance(yaml_config, dict)

    conf: Config = Config()

    if "inputs" not in yaml_config or not isinstance(yaml_config["inputs"], list):
        raise ValueError("No inputs provided")

    for input_config in yaml_config["inputs"]:
        if "type" not in input_config or not isinstance(input_config["type"], str):
            raise ValueError("Must be provided str type for input")

        if "id" not in input_config or not isinstance(input_config["id"], str):
            raise ValueError("Must be provided str id for input")

        current_input: Input = Input(input_type=input_config["type"], input_id=input_config["id"])

        if "outputs" not in input_config or not isinstance(input_config["outputs"], list):
            raise ValueError("No valid outputs for input")

        for output_config in input_config["outputs"]:
            if "type" not in output_config or not isinstance(output_config["type"], str):
                raise ValueError("Must be provided str type for output")

            if "args" not in output_config or not isinstance(output_config["args"], dict):
                raise ValueError("Must be provided dict args for output")

            current_input.add_output(output_type=output_config["type"], output_kwargs=output_config["args"])

        conf.add_input(current_input)

    return conf
