# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Optional

import yaml

from .include_exlude import IncludeExcludeFilter, IncludeExcludeRule
from .logger import logger as shared_logger

_available_input_types: list[str] = ["cloudwatch-logs", "s3-sqs", "sqs", "kinesis-data-stream"]
_available_output_types: list[str] = ["elasticsearch"]


class Output:
    """
    Base class for Output component
    """

    def __init__(self, output_type: str):
        self.type: str = output_type

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Output type must be of type str")

        if value not in _available_output_types:
            raise ValueError(f"Type must be one of {','.join(_available_output_types)}")
        self._type = value


class ElasticsearchOutput(Output):
    def __init__(
        self,
        elasticsearch_url: str = "",
        cloud_id: str = "",
        username: str = "",
        password: str = "",
        api_key: str = "",
        es_index_or_datastream_name: str = "",
        tags: list[str] = [],
        batch_max_actions: int = 500,
        batch_max_bytes: int = 10 * 1024 * 1024,
    ):

        super().__init__(output_type="elasticsearch")
        self.elasticsearch_url = elasticsearch_url
        self.cloud_id = cloud_id
        self.username = username
        self.password = password
        self.api_key = api_key
        self.es_index_or_datastream_name = es_index_or_datastream_name
        self.tags = tags
        self.batch_max_actions = batch_max_actions
        self.batch_max_bytes = batch_max_bytes

        if not self.cloud_id and not self.elasticsearch_url:
            raise ValueError("Elasticsearch Output elasticsearch_url or cloud_id must be set")

        if self.cloud_id and self.elasticsearch_url:
            shared_logger.warning("both elasticsearch_url and cloud_id set in config: using elasticsearch_url")
            self.cloud_id = ""

        if not self.username and not self.api_key:
            raise ValueError("Elasticsearch Output username and password or api_key must be set")

        if self.username and self.api_key:
            shared_logger.warning("both api_key and username and password set in config: using api_key")
            self._username = ""
            self._password = ""

        if self.username and not self.password:
            raise ValueError("Elasticsearch Output password must be set when using username")

        if not self.es_index_or_datastream_name:
            shared_logger.info("no es_index_or_datastream_name set in config")

        shared_logger.debug("tags: ", extra={"tags": self.tags})

    @property
    def elasticsearch_url(self) -> str:
        return self._elasticsearch_url

    @elasticsearch_url.setter
    def elasticsearch_url(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output elasticsearch_url must be of type str")

        self._elasticsearch_url = value

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output username must be of type str")

        self._username = value

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output password must be of type str")

        self._password = value

    @property
    def cloud_id(self) -> str:
        return self._cloud_id

    @cloud_id.setter
    def cloud_id(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output cloud_id must be of type str")

        self._cloud_id = value

    @property
    def api_key(self) -> str:
        return self._api_key

    @api_key.setter
    def api_key(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output api_key must be of type str")

        self._api_key = value

    @property
    def es_index_or_datastream_name(self) -> str:
        return self._es_index_or_datastream_name

    @es_index_or_datastream_name.setter
    def es_index_or_datastream_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Elasticsearch Output es_index_or_datastream_name must be of type str")

        self._es_index_or_datastream_name = value

    @property
    def batch_max_actions(self) -> int:
        return self._batch_max_actions

    @batch_max_actions.setter
    def batch_max_actions(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("Elasticsearch Output batch_max_actions must be of type int")

        self._batch_max_actions = value

    @property
    def batch_max_bytes(self) -> int:
        return self._batch_max_bytes

    @batch_max_bytes.setter
    def batch_max_bytes(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("Elasticsearch Output batch_max_bytes must be of type int")

        self._batch_max_bytes = value


class Input:
    """
    Base class for Input component
    """

    def __init__(self, input_type: str, input_id: str):
        self.type = input_type
        self.id = input_id
        self._tags: list[str] = []
        self._outputs: dict[str, Output] = {}
        self._include_exclude_filter: Optional[IncludeExcludeFilter] = None

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Input type must be of type str")

        if value not in _available_input_types:
            raise ValueError(f"Input type must be one of {','.join(_available_input_types)}")
        self._type = value

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Input id must be of type str")
        self._id = value

    @property
    def tags(self) -> list[str]:
        """
        Tags getter.
        Returns all tags
        """
        return self._tags

    @tags.setter
    def tags(self, values: list[str]) -> None:
        """
        Tags setter.
        It receives a list of tags and performs type validation
        """
        if not isinstance(values, list):
            raise ValueError("Tags must be of type list")

        self._tags = [value for value in values if isinstance(value, str)]
        if len(self._tags) != len(values):
            raise ValueError(f"Each tag must be of type str, given: {values}")

    @property
    def include_exclude_filter(self) -> Optional[IncludeExcludeFilter]:
        return self._include_exclude_filter

    @include_exclude_filter.setter
    def include_exclude_filter(self, value: IncludeExcludeFilter) -> None:
        if not isinstance(value, IncludeExcludeFilter):
            raise ValueError("An error occurred while setting include and exclude filter")

        self._include_exclude_filter = value

    def get_output_by_type(self, output_type: str) -> Optional[Output]:
        """
        Output getter.
        Returns a specific output given its type
        """

        return self._outputs[output_type] if output_type in self._outputs else None

    def get_output_types(self) -> list[str]:
        """
        Output types getter.
        Returns all the defined output types
        """

        return list(self._outputs.keys())

    def delete_output_by_type(self, output_type: str) -> None:
        """
        Output deleter.
        Delete a defined output by its type
        """

        del self._outputs[output_type]

    def add_output(self, output_type: str, **kwargs: Any) -> None:
        """
        Output setter.
        Set an output given its type and init kwargs
        """
        if not isinstance(output_type, str):
            raise ValueError("Output type must be of type str")

        if output_type in self._outputs:
            raise ValueError(f"Duplicated Output {output_type}")

        output: Optional[Output] = None
        if output_type == "elasticsearch":
            output = ElasticsearchOutput(**kwargs)

        assert output is not None
        self._outputs[output.type] = output


class Config:
    """
    Config component
    """

    def __init__(self) -> None:
        self._inputs: dict[str, dict[str, Input]] = {}

    def get_input_by_type_and_id(self, input_type: str, input_id: str) -> Optional[Input]:
        """
        Input getter.
        Returns a specific input given its type and id
        """

        if input_type not in self._inputs:
            return None

        return self._inputs[input_type][input_id] if input_id in self._inputs[input_type] else None

    def add_input(self, new_input: Input) -> None:
        """
        Input setter.
        Set an input.
        """

        if new_input.type not in self._inputs:
            self._inputs[new_input.type] = {new_input.id: new_input}

            return

        if new_input.id in self._inputs[new_input.type]:
            raise ValueError(f"duplicated input {new_input.type}/{new_input.id}")

        self._inputs[new_input.type][new_input.id] = new_input


def parse_config(config_yaml: str, expanders: list[Callable[[str], str]] = []) -> Config:
    """
    Config component factory
    Given a config yaml as string it return the Config instance as defined by the yaml
    """

    for expander in expanders:
        config_yaml = expander(config_yaml)

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

        if "tags" in input_config:
            current_input.tags = input_config["tags"]

        include_rules: list[IncludeExcludeRule] = []
        if "include" in input_config:
            include_rules_from_config = input_config["include"]
            if not isinstance(include_rules_from_config, list):
                raise ValueError("Must be provided list type for include")

            for i, include_rule in enumerate(include_rules_from_config):
                if not isinstance(include_rule, dict) or "key" not in include_rule or "pattern" not in include_rule:
                    raise ValueError(f"Must be provided dict with `key` and `pattern` fields for include rule #{i}")

                if not isinstance(include_rule["key"], str):
                    raise ValueError(f"Must be provided str type for `key` field for include rule #{i}")

                if not isinstance(include_rule["pattern"], str):
                    raise ValueError(f"Must be provided str type for `pattern` field for include rule #{i}")

                include_rules.append(IncludeExcludeRule(path_key=include_rule["key"], pattern=include_rule["pattern"]))

        exclude_rules: list[IncludeExcludeRule] = []
        if "exclude" in input_config:
            exclude_rules_from_config = input_config["exclude"]
            if not isinstance(exclude_rules_from_config, list):
                raise ValueError("Must be provided list type for exclude")

            for i, exclude_rule in enumerate(exclude_rules_from_config):
                if not isinstance(exclude_rule, dict) or "key" not in exclude_rule or "pattern" not in exclude_rule:
                    raise ValueError(f"Must be provided dict with `key` and `pattern` fields for exclude rule #{i}")

                if not isinstance(exclude_rule["key"], str):
                    raise ValueError(f"Must be provided str type for `key` field for exclude rule #{i}")

                if not isinstance(exclude_rule["pattern"], str):
                    raise ValueError(f"Must be provided str type for `pattern` field for exclude rule #{i}")

                exclude_rules.append(IncludeExcludeRule(path_key=exclude_rule["key"], pattern=exclude_rule["pattern"]))

        if len(include_rules) > 0 or len(exclude_rules) > 0:
            current_input.include_exclude_filter = IncludeExcludeFilter(
                include_patterns=include_rules, exclude_patterns=exclude_rules
            )

        if "outputs" not in input_config or not isinstance(input_config["outputs"], list):
            raise ValueError("No valid outputs for input")

        for output_config in input_config["outputs"]:
            if "type" not in output_config or not isinstance(output_config["type"], str):
                raise ValueError("Must be provided str type for output")

            if "args" not in output_config or not isinstance(output_config["args"], dict):
                raise ValueError("Must be provided dict args for output")

            output_config["args"]["tags"] = current_input.tags

            current_input.add_output(output_type=output_config["type"], **output_config["args"])

        conf.add_input(current_input)

    return conf
