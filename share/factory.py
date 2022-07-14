# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable

from .multiline import CountMultiline, PatternMultiline, ProtocolMultiline, WhileMultiline

_init_definition_by_multiline_type: dict[str, dict[str, Any]] = {
    "count": {
        "class": CountMultiline,
    },
    "pattern": {
        "class": PatternMultiline,
    },
    "while_pattern": {
        "class": WhileMultiline,
    },
}


class MultilineFactory:
    """
    Multiline factory.
    Provides a static method to instantiate a multiline processor
    """

    @staticmethod
    def create(multiline_type: str, **kwargs: Any) -> ProtocolMultiline:
        """
        Instantiates a concrete Multiline processor given a multiline type and args
        """

        if multiline_type not in _init_definition_by_multiline_type:
            raise ValueError(
                "You must provide one of the following multiline types: "
                + f"{', '.join(_init_definition_by_multiline_type.keys())}. {multiline_type} given"
            )

        multiline_definition = _init_definition_by_multiline_type[multiline_type]

        multiline_builder: Callable[..., ProtocolMultiline] = multiline_definition["class"]

        return multiline_builder(**kwargs)
