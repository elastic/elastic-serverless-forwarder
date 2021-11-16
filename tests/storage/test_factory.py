# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

import pytest

from storage import CommonStorage, S3Storage, StorageFactory


@pytest.mark.unit
class TestStorageFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create s3 storage success"):
            storage: CommonStorage = StorageFactory.create(
                storage_type="s3", bucket_name="bucket_name", object_key="object_key"
            )

            assert isinstance(storage, S3Storage)

        with self.subTest("create s3 storage error"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "You must provide the following not empty init kwargs for"
                    + " s3: bucket_name, object_key. (provided: {})"
                ),
            ):
                StorageFactory.create(storage_type="s3")

        with self.subTest("create s3 storage empty kwargs"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "You must provide the following not empty init kwargs for s3: bucket_name, object_key."
                    + ' (provided: {"bucket_name": "", "object_key": ""})'
                ),
            ):
                StorageFactory.create(storage_type="s3", bucket_name="", object_key="")

        with self.subTest("create invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following " + "storage types: s3")
            ):
                StorageFactory.create(storage_type="invalid type")
