# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import re
from unittest import TestCase

from share import ElasticsearchOutput, LogstashOutput, Output
from shippers import ElasticsearchShipper, LogstashShipper, ProtocolShipper, ShipperFactory


class TestShipperFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create elasticsearch shipper success elasticsearch_url and http auth"):
            shipper: ProtocolShipper = ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success elasticsearch_url and api key"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                elasticsearch_url="elasticsearch_url",
                api_key="api_key",
                es_datastream_name="es_datastream_name",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and http auth"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                username="username",
                password="password",
                es_datastream_name="es_datastream_name",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create elasticsearch shipper success cloud id and api key"):
            shipper = ShipperFactory.create(
                output_type="elasticsearch",
                cloud_id="cloud_id:bG9jYWxob3N0OjkyMDAkMA==",
                api_key="api_key",
                es_datastream_name="es_datastream_name",
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create logstash shipper success with only logstash_url"):
            shipper = ShipperFactory.create(
                output_type="logstash",
                logstash_url="http://myhost:8080",
            )

            assert isinstance(shipper, LogstashShipper)

        with self.subTest("create logstash shipper success with logstash_url, batch size and compression level"):
            shipper = ShipperFactory.create(
                output_type="logstash",
                logstash_url="http://myhost:8080",
                max_batch_size=50,
                compression_level=9,
            )

            assert isinstance(shipper, LogstashShipper)
        with self.subTest("create elasticsearch shipper no kwargs error"):
            with self.assertRaisesRegex(ValueError, "You must provide one between elasticsearch_url or cloud_id"):
                ShipperFactory.create(output_type="elasticsearch")

        with self.subTest("create logstash shipper no kwargs error"):
            with self.assertRaisesRegex(ValueError, "You must provide logstash_url"):
                ShipperFactory.create(output_type="logstash")

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

        with self.subTest("create logstash shipper compression level lower than 0"):
            with self.assertRaisesRegex(ValueError, "compression_level must be an integer value between 0 and 9"):
                ShipperFactory.create(output_type="logstash", logstash_url="logstash_url", compression_level=-1)

        with self.subTest("create logstash shipper compression level higher than 9"):
            with self.assertRaisesRegex(ValueError, "compression_level must be an integer value between 0 and 9"):
                ShipperFactory.create(output_type="logstash", logstash_url="logstash_url", compression_level=10)

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
            es_datastream_name="es_datastream_name",
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
            shipper: ProtocolShipper = ShipperFactory.create_from_output(
                output_type=elasticsearch_output.type, output=elasticsearch_output
            )

            assert isinstance(shipper, ElasticsearchShipper)

        with self.subTest("create from output invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following outputs: elasticsearch, logstash")
            ):
                ShipperFactory.create_from_output(output_type="invalid type", output=elasticsearch_output)

        logstash_output = LogstashOutput(logstash_url="logstash_url")

        with self.subTest("create output type logstash"):
            with self.assertRaisesRegex(
                ValueError,
                re.escape("output expected to be LogstashOutput type, given <class 'share.config.Output'>"),
            ):
                ShipperFactory.create_from_output(output_type="logstash", output=Output(output_type="logstash"))

        with self.subTest("create from output logstash shipper success"):
            logstash_shipper: ProtocolShipper = ShipperFactory.create_from_output(
                output_type=logstash_output.type, output=logstash_output
            )

            assert isinstance(logstash_shipper, LogstashShipper)

        with self.subTest("create from output invalid type"):
            with self.assertRaisesRegex(
                ValueError, re.escape("You must provide one of the following outputs: elasticsearch, logstash")
            ):
                ShipperFactory.create_from_output(output_type="invalid type", output=logstash_output)
