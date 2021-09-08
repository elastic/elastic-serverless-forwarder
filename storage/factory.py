import json
from typing import Generator

from storage.s3 import S3Storage
from storage.storage import CommonStorage


class StorageFactory:
    _init_definition_by_storage_type: dict[str, dict[str, any]] = {
        "s3": {"class": S3Storage, "kwargs": ["bucket_name", "object_key"]}
    }

    def __init__(self, storage_type: str, **kwargs: any):
        if storage_type not in self._init_definition_by_storage_type:
            raise ValueError(
                f"""
                you must provide one of the following storage types:
                    {", ".join(self._init_definition_by_storage_type.keys())}
            """
            )

        init_kwargs: list[str] = [
            key
            for key in kwargs.keys()
            if key in self._init_definition_by_storage_type[storage_type]["kwargs"] and kwargs[key]
        ]
        if len(init_kwargs) is not len(self._init_definition_by_storage_type[storage_type]["kwargs"]):
            raise ValueError(
                f"""
                you must provide the following not empty init kwargs for {storage_type}:
                    {", ".join(self._init_definition_by_storage_type[storage_type]["kwargs"])}.
                (provided: {json.dumps(kwargs)})
            """
            )

        self._storage: CommonStorage = self._init_definition_by_storage_type[storage_type]["class"](**kwargs)

    def get(self) -> Generator[tuple[bytes, int], None, None]:
        return self._storage.get()
