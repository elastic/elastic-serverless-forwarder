# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

from share import ElasticSearchOutput
from shippers import CommonShipper, ElasticsearchShipper, ShipperFactory


class TestShipperFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create elasticsearch shipper success hosts and http auth"):
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

        with self.subTest("create elasticsearch shipper success hosts and api key"):
            shipper = ShipperFactory.create(
                output="elasticsearch",
                hosts=["hosts"],
                scheme="scheme",
                api_key="api_key",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and http auth"):
            shipper = ShipperFactory.create(
                output="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                username="username",
                password="password",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and api key"):
            shipper = ShipperFactory.create(
                output="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                api_key="api_key",
                dataset="dataset",
                namespace="namespace",
            )

            assert isinstance(shipper, ElasticsearchShipper)
        with self.subTest("create elasticsearch shipper no kwargs error"):
            with self.assertRaisesRegex(ValueError, "You must provide one between hosts and scheme or cloud_id"):
                ShipperFactory.create(output="elasticsearch")

        with self.subTest("create elasticsearch shipper empty hosts and no cloud_id"):
            with self.assertRaisesRegex(ValueError, "You must provide one between hosts and scheme or cloud_id"):
                ShipperFactory.create(output="elasticsearch", hosts=[])

        with self.subTest("create elasticsearch shipper empty cloud_id and no hosts"):
            with self.assertRaisesRegex(ValueError, "You must provide one between hosts and scheme or cloud_id"):
                ShipperFactory.create(output="elasticsearch", cloud_id="")

        with self.subTest("create elasticsearch shipper empty username and no api_key"):
            with self.assertRaisesRegex(ValueError, "You must provide one between username and password or api_key"):
                ShipperFactory.create(output="elasticsearch", hosts=["hosts"], username="")

        with self.subTest("create elasticsearch shipper empty api_key and no username"):
            with self.assertRaisesRegex(ValueError, "You must provide one between username and password or api_key"):
                ShipperFactory.create(output="elasticsearch", hosts=["hosts"], api_key="")

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
