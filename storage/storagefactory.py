from typing import Generator

from storage import CommonStorage, S3Storage


class StorageFactory:
    _available_types: list[str] = ["s3"]
    _init_kwargs_by_type: dict[str, list[str]] = {
        "s3": ["bucket_arn", "object_key"]
    }

    def __init__(self, storage_type: str, **kwargs: any):
        if storage_type not in self._available_types:
            raise ValueError(f'''
                you must provide one of the following types:
                    {", ".join(self._available_types)}
            ''')

        init_kwargs: list[str] = [key for key in kwargs.keys() if key in self._init_kwargs_by_type[storage_type]]
        if len(init_kwargs) is not len(self._init_kwargs_by_type[storage_type]):
            raise ValueError(f'''
                you must provide the following init kwargs for {storage_type}:
                    {", ".join(self._init_kwargs_by_type[storage_type])}.
                (provided: {", ".join(kwargs.keys())})
            ''')

        if storage_type == "s3":
            self._storage: CommonStorage = S3Storage(bucket_arn=kwargs["bucket_arn"], object_key=kwargs["object_key"])

    def get(self) -> Generator[(bytes, int), None, None]:
        return self._storage.get()
