# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Optional, Union

import yaml

from .factory import MultilineFactory
from .include_exlude import IncludeExcludeFilter, IncludeExcludeRule
from .logger import logger as shared_logger
from .multiline import ProtocolMultiline

_available_input_types: list[str] = ["cloudwatch-logs", "s3-sqs", "sqs", "kinesis-data-stream"]
_available_output_types: list[str] = ["elasticsearch", "logstash"]

IntegrationScopeDiscovererCallable = Callable[[dict[str, Any], int], str]


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
            raise ValueError("`type` must be provided as string")

        if value not in _available_output_types:
            raise ValueError(f"`type` must be one of {','.join(_available_output_types)}: {value} given")
        self._type = value


class ElasticsearchOutput(Output):
    def __init__(
        self,
        elasticsearch_url: str = "",
        cloud_id: str = "",
        username: str = "",
        password: str = "",
        api_key: str = "",
        es_datastream_name: str = "",
        tags: list[str] = [],
        batch_max_actions: int = 500,
        batch_max_bytes: int = 10 * 1024 * 1024,
        ssl_assert_fingerprint: str = "",
    ):
        super().__init__(output_type="elasticsearch")
        self.elasticsearch_url = elasticsearch_url
        self.cloud_id = cloud_id
        self.username = username
        self.password = password
        self.api_key = api_key
        self.es_datastream_name = es_datastream_name
        self.tags = tags
        self.batch_max_actions = batch_max_actions
        self.batch_max_bytes = batch_max_bytes
        self.ssl_assert_fingerprint = ssl_assert_fingerprint

        if not self.cloud_id and not self.elasticsearch_url:
            raise ValueError("One between `elasticsearch_url` or `cloud_id` must be set")

        if self.cloud_id and self.elasticsearch_url:
            shared_logger.warning("both `elasticsearch_url` and `cloud_id` set in config: using `elasticsearch_url`")
            self.cloud_id = ""

        if not self.username and not self.api_key:
            raise ValueError("One between `username` and `password`, or `api_key` must be set")

        if self.username and self.api_key:
            shared_logger.warning("both `api_key` and `username` and `password` set in config: using `api_key`")
            self._username = ""
            self._password = ""

        if self.username and not self.password:
            raise ValueError("`password` must be set when using `username`")

        if not self.es_datastream_name:
            shared_logger.debug("no `es_datastream_name` set in config")

        shared_logger.debug("tags: ", extra={"tags": self.tags})

    @property
    def elasticsearch_url(self) -> str:
        return self._elasticsearch_url

    @elasticsearch_url.setter
    def elasticsearch_url(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`elasticsearch_url` must be provided as string")

        self._elasticsearch_url = value

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`username` must be provided as string")

        self._username = value

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`password` must be provided as string")

        self._password = value

    @property
    def cloud_id(self) -> str:
        return self._cloud_id

    @cloud_id.setter
    def cloud_id(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`cloud_id` must be provided as string")

        self._cloud_id = value

    @property
    def api_key(self) -> str:
        return self._api_key

    @api_key.setter
    def api_key(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`api_key` must be provided as string")

        self._api_key = value

    @property
    def es_datastream_name(self) -> str:
        return self._es_datastream_name

    @es_datastream_name.setter
    def es_datastream_name(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`es_datastream_name` must be provided as string")

        self._es_datastream_name = value

    @property
    def batch_max_actions(self) -> int:
        return self._batch_max_actions

    @batch_max_actions.setter
    def batch_max_actions(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("`batch_max_actions` must be provided as integer")

        self._batch_max_actions = value

    @property
    def batch_max_bytes(self) -> int:
        return self._batch_max_bytes

    @batch_max_bytes.setter
    def batch_max_bytes(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("`batch_max_bytes` must be provided as integer")

        self._batch_max_bytes = value

    @property
    def ssl_assert_fingerprint(self) -> str:
        return self._ssl_assert_fingerprint

    @ssl_assert_fingerprint.setter
    def ssl_assert_fingerprint(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`ssl_assert_fingerprint` must be provided as string")

        self._ssl_assert_fingerprint = value


class LogstashOutput(Output):
    def __init__(
        self,
        logstash_url: str = "",
        username: str = "",
        password: str = "",
        max_batch_size: int = 500,
        compression_level: int = 9,
        tags: list[str] = [],
        ssl_assert_fingerprint: str = "",
    ) -> None:
        super().__init__(output_type="logstash")
        self.logstash_url = logstash_url
        self.username = username
        self.password = password
        self.max_batch_size = max_batch_size
        self.compression_level = compression_level
        self.tags = tags
        self.ssl_assert_fingerprint = ssl_assert_fingerprint

        if self.username and not self.password:
            raise ValueError("`password` must be set when using `username`")

        shared_logger.debug("tags: ", extra={"tags": self.tags})

    @property
    def logstash_url(self) -> str:
        return self._logstash_url

    @logstash_url.setter
    def logstash_url(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`logstash_url` must be provided as string")

        self._logstash_url = value

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`username` must be provided as string")

        self._username = value

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`password` must be provided as string")

        self._password = value

    @property
    def max_batch_size(self) -> int:
        return self._max_batch_size

    @max_batch_size.setter
    def max_batch_size(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("`max_batch_size` must be provided as int")

        self._max_batch_size = value

    @property
    def compression_level(self) -> int:
        return self._compression_level

    @compression_level.setter
    def compression_level(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("`compression_level` must be provided as int")

        self._compression_level = value

    @property
    def ssl_assert_fingerprint(self) -> str:
        return self._ssl_assert_fingerprint

    @ssl_assert_fingerprint.setter
    def ssl_assert_fingerprint(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`ssl_assert_fingerprint` must be provided as string")

        self._ssl_assert_fingerprint = value


class Input:
    """
    Base class for Input component
    """

    def __init__(self, input_type: str, input_id: str):
        self.id = input_id
        self.type = input_type

        self._tags: list[str] = []
        self._json_content_type: str = ""
        self._expand_event_list_from_field: str = ""
        self._root_fields_to_add_to_expanded_event: Optional[Union[str, list[str]]] = None
        self._outputs: dict[str, Output] = {}

        self._multiline_processor: Optional[ProtocolMultiline] = None
        self._include_exclude_filter: Optional[IncludeExcludeFilter] = None

        self._valid_json_content_type: list[str] = ["ndjson", "single", "disabled"]

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`type` must be provided as string")

        if value not in _available_input_types:
            raise ValueError(f"`type` must be one of {','.join(_available_input_types)}: {value} given")
        self._type = value

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("`id` must be provided as string")
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
            raise ValueError(f"`tags` must be provided as list for input {self.id}")

        self._tags = [value for value in values if isinstance(value, str)]
        if len(self._tags) != len(values):
            raise ValueError(f"Each tag in `tags` must be provided as string for input {self.id}, given: {values}")

    @property
    def expand_event_list_from_field(self) -> str:
        return self._expand_event_list_from_field

    @expand_event_list_from_field.setter
    def expand_event_list_from_field(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError(f"`expand_event_list_from_field` must be provided as string for input {self.id}")

        self._expand_event_list_from_field = value

    @property
    def root_fields_to_add_to_expanded_event(self) -> Optional[Union[str, list[str]]]:
        return self._root_fields_to_add_to_expanded_event

    @root_fields_to_add_to_expanded_event.setter
    def root_fields_to_add_to_expanded_event(self, value: Union[str, list[str]]) -> None:
        if isinstance(value, str) and value == "all":
            self._root_fields_to_add_to_expanded_event = value
            return

        if isinstance(value, list):
            self._root_fields_to_add_to_expanded_event = value
            return

        raise ValueError("`root_fields_to_add_to_expanded_event` must be provided as `all` or a list of strings")

    @property
    def json_content_type(self) -> str:
        return self._json_content_type

    @json_content_type.setter
    def json_content_type(self, value: str) -> None:
        if value not in self._valid_json_content_type:
            raise ValueError(
                f"`json_content_type` must be one of {','.join(self._valid_json_content_type)} "
                f"for input {self.id}: {value} given"
            )

        self._json_content_type = value

    @property
    def include_exclude_filter(self) -> Optional[IncludeExcludeFilter]:
        return self._include_exclude_filter

    @include_exclude_filter.setter
    def include_exclude_filter(self, value: IncludeExcludeFilter) -> None:
        if not isinstance(value, IncludeExcludeFilter):
            raise ValueError(f"An error occurred while setting include and exclude filter for input {self.id}")

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
            raise ValueError("`type` must be provided as string")

        if output_type in self._outputs:
            raise ValueError(f"Duplicated `type` {output_type}")

        output: Optional[Output] = None
        if output_type == "elasticsearch":
            output = ElasticsearchOutput(**kwargs)
        elif output_type == "logstash":
            output = LogstashOutput(**kwargs)
        else:
            output = Output(output_type=output_type)

        self._outputs[output.type] = output

    def get_multiline_processor(self) -> Optional[ProtocolMultiline]:
        return self._multiline_processor

    def add_multiline_processor(self, multiline_type: str, **kwargs: Any) -> None:
        """
        Multiline setter.
        Set a multiline processor given its type and init kwargs
        """

        self._multiline_processor = MultilineFactory.create(multiline_type=multiline_type, **kwargs)


class Config:
    """
    Config component
    """

    def __init__(self) -> None:
        self._inputs: dict[str, Input] = {}

    @property
    def inputs(self) -> dict[str, Input]:
        """
        Inputs getter.
        Returns all inputs
        """
        return self._inputs

    def get_input_by_id(self, input_id: str) -> Optional[Input]:
        """
        Input getter.
        Returns a specific input given its id
        """

        if input_id in self._inputs:
            return self._inputs[input_id]

        return None

    def add_input(self, new_input: Input) -> None:
        """
        Input setter.
        Set an input.
        """

        if new_input.id in self._inputs:
            raise ValueError(f"Duplicated input with id {new_input.id}")

        self._inputs[new_input.id] = new_input


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
        raise ValueError("`inputs` must be provided as list")

    for input_n, input_config in enumerate(yaml_config["inputs"]):
        if "id" not in input_config or not isinstance(input_config["id"], str):
            raise ValueError(f"`id` must be provided as string for input at position {input_n + 1}")

        if "type" not in input_config or not isinstance(input_config["type"], str):
            raise ValueError(f'`type` must be provided as string for input {input_config["id"]}')

        try:
            current_input: Input = Input(input_type=input_config["type"], input_id=input_config["id"])
        except ValueError as e:
            raise ValueError(f'An error occurred while applying type configuration for input {input_config["id"]}: {e}')

        if "tags" in input_config:
            current_input.tags = input_config["tags"]

        if "multiline" in input_config:
            if not isinstance(input_config["multiline"], dict):
                raise ValueError(f'`multiline` must be provided as dictionary for input {input_config["id"]}')

            multiline_config = input_config["multiline"]
            if "type" not in multiline_config or not isinstance(multiline_config["type"], str):
                raise ValueError(
                    f'`type` must be provided as string in multiline configuration for input {input_config["id"]}'
                )

            multiline_config["multiline_type"] = multiline_config["type"]
            del multiline_config["type"]

            try:
                current_input.add_multiline_processor(**multiline_config)
            except ValueError as e:
                raise ValueError(
                    f'An error occurred while applying multiline configuration for input {input_config["id"]}: {e}'
                )

        if "expand_event_list_from_field" in input_config:
            current_input.expand_event_list_from_field = input_config["expand_event_list_from_field"]

        if "root_fields_to_add_to_expanded_event" in input_config:
            current_input.root_fields_to_add_to_expanded_event = input_config["root_fields_to_add_to_expanded_event"]

        if "json_content_type" in input_config:
            current_input.json_content_type = input_config["json_content_type"]

        include_rules: list[IncludeExcludeRule] = []
        if "include" in input_config:
            include_rules_from_config = input_config["include"]
            if not isinstance(include_rules_from_config, list):
                raise ValueError(f'`include` must be provided as list for input {input_config["id"]}')

            for include_rule in include_rules_from_config:
                include_rules.append(IncludeExcludeRule(pattern=str(include_rule)))

        exclude_rules: list[IncludeExcludeRule] = []
        if "exclude" in input_config:
            exclude_rules_from_config = input_config["exclude"]
            if not isinstance(exclude_rules_from_config, list):
                raise ValueError(f'`exclude` must be provided as list for input {input_config["id"]}')

            for exclude_rule in exclude_rules_from_config:
                exclude_rules.append(IncludeExcludeRule(pattern=str(exclude_rule)))

        if len(include_rules) > 0 or len(exclude_rules) > 0:
            current_input.include_exclude_filter = IncludeExcludeFilter(
                include_patterns=include_rules, exclude_patterns=exclude_rules
            )

        if "outputs" not in input_config or not isinstance(input_config["outputs"], list):
            raise ValueError(f'`outputs` must be provided as list for input {input_config["id"]}')

        for output_n, output_config in enumerate(input_config["outputs"]):
            if "type" not in output_config or not isinstance(output_config["type"], str):
                raise ValueError(
                    f"`type` for output configuration at position {output_n + 1} must "
                    f'be provided as string for input {input_config["id"]}'
                )

            if "args" not in output_config or not isinstance(output_config["args"], dict):
                raise ValueError(
                    f"`args` for output configuration at position {output_n + 1} "
                    f'must be provided as dictionary for input {input_config["id"]}'
                )

            output_config["args"]["tags"] = current_input.tags

            try:
                current_input.add_output(output_type=output_config["type"], **output_config["args"])
            except ValueError as e:
                raise ValueError(
                    f"An error occurred while applying output configuration "
                    f'at position {output_n + 1} for input {input_config["id"]}: {e}'
                )

        conf.add_input(current_input)

    return conf
