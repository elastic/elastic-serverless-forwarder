# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
from typing import Any, Callable

from .s3 import S3Storage
from .storage import CommonStorage

_init_definition_by_storage_type: dict[str, dict[str, Any]] = {
    "s3": {"class": S3Storage, "kwargs": ["bucket_name", "object_key"]}
}


class StorageFactory:
    @staticmethod
    def create(storage_type: str, **kwargs: Any) -> CommonStorage:
        if storage_type not in _init_definition_by_storage_type:
            raise ValueError(
                "You must provide one of the following storage types: "
                + f"{', '.join(_init_definition_by_storage_type.keys())}"
            )

        storage_definition = _init_definition_by_storage_type[storage_type]

        storage_kwargs = storage_definition["kwargs"]
        storage_builder: Callable[..., CommonStorage] = storage_definition["class"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in storage_kwargs and kwargs[key]]
        if len(init_kwargs) != len(storage_kwargs):
            raise ValueError(
                f"You must provide the following not empty init kwargs for {storage_type}: "
                + f"{', '.join(storage_kwargs)}. (provided: {json.dumps(kwargs)})"
            )

        return storage_builder(**kwargs)
