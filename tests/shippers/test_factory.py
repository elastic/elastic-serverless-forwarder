# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

from share import ElasticsearchOutput, Output
from shippers import CommonShipper, ElasticsearchShipper, ShipperFactory


class TestShipperFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create elasticsearch shipper success elasticsearch_url and http auth"):
            shipper: CommonShipper = ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                datastream="datastream",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success elasticsearch_url and api key"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                api_key="api_key",
                datastream="datastream",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and http auth"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                username="username",
                password="password",
                datastream="datastream",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and api key"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                api_key="api_key",
                datastream="datastream",
            )

            assert isinstance(shipper, ElasticsearchShipper)
        with self.subTest("create elasticsearch shipper no kwargs error"):
            with self.assertRaisesRegex(ValueError, "You must provide one between elasticsearch_url or cloud_id"):
                ShipperFactory.create(output_type="elasticsearch")

        with self.subTest("create elasticsearch shipper empty elasticsearch_url and no cloud_id"):
            with self.assertRaisesRegex(ValueError, "You must provide one between elasticsearch_url or cloud_id"):
                ShipperFactory.create(output_type="elasticsearch", elasticsearch_url="")

        with self.subTest("create elasticsearch shipper empty cloud_id and no elasticsearch_url"):
            with self.assertRaisesRegex(ValueError, "You must provide one between elasticsearch_url or cloud_id"):
                ShipperFactory.create(output_type="elasticsearch", cloud_id="")

        with self.subTest("create elasticsearch shipper empty username and no api_key"):
            with self.assertRaisesRegex(ValueError, "You must provide one between username and password or api_key"):
                ShipperFactory.create(output_type="elasticsearch", elasticsearch_url="elasticsearch_url", username="")

        with self.subTest("create elasticsearch shipper empty api_key and no username"):
            with self.assertRaisesRegex(ValueError, "You must provide one between username and password or api_key"):
                ShipperFactory.create(output_type="elasticsearch", elasticsearch_url="elasticsearch_url", api_key="")

        with self.subTest("create invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following outputs: elasticsearch")
            ):
                ShipperFactory.create(output_type="invalid type")

    def test_create_from_output(self) -> None:
        elasticsearch_output = ElasticsearchOutput(
            elasticsearch_url="elasticsearch_url",
            username="username",
            password="password",
            datastream="datastream",
        )

        with self.subTest("create output type elasticsearch"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape("output expected to be ElasticsearchOutput type, given <class 'share.config.Output'>"),
            ):
                ShipperFactory.create_from_output(
                    output_type="elasticsearch", output=Output(output_type="elasticsearch")
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
