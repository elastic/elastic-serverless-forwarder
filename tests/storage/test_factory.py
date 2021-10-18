import re
from unittest import TestCase

from storage import CommonStorage, S3Storage, StorageFactory


class TestStorageFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create s3 storage success"):
            config_storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name="bucket_name", object_key="object_key"
            )

            assert isinstance(config_storage, S3Storage)

        with self.subTest("create s3 storage error"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "You must provide the following not empty init kwargs for"
                    + " s3: bucket_name, object_key. (provided: {})"
                ),
            ):
                StorageFactory.create(storage_type="s3")

        with self.subTest("create invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following " + "storage types: s3")
            ):
                StorageFactory.create(storage_type="invalid type")
