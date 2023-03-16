# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Callable, Optional

from share import ExpandEventListFromField, ProtocolMultiline, json_dumper

from .payload import PayloadStorage
from .s3 import S3Storage
from .storage import ProtocolStorage

_init_definition_by_storage_type: dict[str, dict[str, Any]] = {
    "s3": {"class": S3Storage, "kwargs": ["bucket_name", "object_key"]},
    "payload": {"class": PayloadStorage, "kwargs": ["payload"]},
}


class StorageFactory:
    """
    Storage factory.
    Provides static methods to instantiate a Storage
    """

    @staticmethod
    def create(
        storage_type: str,
        json_content_type: Optional[str] = None,
        event_list_from_field_expander: Optional[ExpandEventListFromField] = None,
        multiline_processor: Optional[ProtocolMultiline] = None,
        **kwargs: Any,
    ) -> ProtocolStorage:
        """
        Instantiates a concrete Storage given its type and the storage init kwargs
        """

        if storage_type not in _init_definition_by_storage_type:
            raise ValueError(
                "You must provide one of the following storage types: "
                + f"{', '.join(_init_definition_by_storage_type.keys())}"
            )

        storage_definition = _init_definition_by_storage_type[storage_type]
        storage_kwargs = storage_definition["kwargs"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in storage_kwargs and kwargs[key]]
        if len(init_kwargs) != len(storage_kwargs):
            raise ValueError(
                f"You must provide the following not empty init kwargs for {storage_type}: "
                + f"{', '.join(storage_kwargs)}. (provided: {json_dumper(kwargs)})"
            )

        kwargs["json_content_type"] = json_content_type
        kwargs["multiline_processor"] = multiline_processor
        kwargs["event_list_from_field_expander"] = event_list_from_field_expander

        storage_builder: Callable[..., ProtocolStorage] = storage_definition["class"]
        return storage_builder(**kwargs)
