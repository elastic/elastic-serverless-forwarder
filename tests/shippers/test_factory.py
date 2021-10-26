# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

from share import ElasticSearchOutput
from shippers import CommonShipper, ElasticsearchShipper, ShipperFactory


class TestShipperFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create elasticsearch shipper success"):
            shipper: CommonShipper = ShipperFactory.create(
                output="elasticsearch",
                hosts=["hosts"],
                scheme="scheme",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper error"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "you must provide the following not empty init kwargs for elasticsearch: hosts, scheme, username,"
                    + " password, dataset, namespace. (provided: {})"
                ),
            ):
                ShipperFactory.create(output="elasticsearch")

        with self.subTest("create elasticsearch shipper empty kwargs"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "you must provide the following not empty init kwargs for elasticsearch: hosts, scheme, username,"
                    + ' password, dataset, namespace. (provided: {"hosts": []})'
                ),
            ):
                ShipperFactory.create(output="elasticsearch", hosts=[])

        with self.subTest("create invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following outputs: elasticsearch")
            ):
                ShipperFactory.create(output="invalid type")

    def test_create_from_output(self) -> None:
        elasticsearch_output = ElasticSearchOutput(
            output_type="elasticsearch",
            kwargs={
                "hosts": ["hosts"],
                "scheme": "scheme",
                "username": "username",
                "password": "password",
                "dataset": "dataset",
                "namespace": "namespace",
            },
        )

        with self.subTest("create from output elasticsearch shipper success"):
            shipper: CommonShipper = ShipperFactory.create_from_output(
                output_type=elasticsearch_output.type, output=elasticsearch_output
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create from output invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following outputs: elasticsearch")
            ):
                ShipperFactory.create_from_output(output_type="invalid type", output=elasticsearch_output)
