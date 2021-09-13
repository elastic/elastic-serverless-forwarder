import json
from typing import Type

from .s3 import S3Storage
from .storage import CommonStorage

_init_definition_by_storage_type: dict[str, dict[str, any]] = {
    "s3": {"class": S3Storage, "kwargs": ["bucket_name", "object_key"]}
}


class StorageFactory:
    @staticmethod
    def create(storage_type: str, **kwargs: any) -> CommonStorage:
        if storage_type not in _init_definition_by_storage_type:
            raise ValueError(
                "You must provide one of the following storage types: "
                + f"{', '.join(_init_definition_by_storage_type.keys())}"
            )

        storage_definition = _init_definition_by_storage_type[storage_type]

        storage_kwargs = storage_definition["kwargs"]
        storage_builder: Type[CommonStorage] = storage_definition["class"]

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in storage_kwargs and kwargs[key]]
        if len(init_kwargs) is not len(storage_kwargs):
            raise ValueError(
                f"You must provide the following not empty init kwargs for {storage_type}: "
                + f"{', '.join(storage_kwargs)}. (provided: {json.dumps(kwargs)})"
            )

        return storage_builder(**kwargs)
