# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest

from handlers.aws.utils import get_shipper_from_input
from share import parse_config
from shippers.logstash import LogstashShipper


@pytest.mark.unit
class TestUtils(TestCase):
    def test_get_shipper_from_input(self) -> None:
        with self.subTest("Logstash shipper from Kinesis input"):
            config_yaml_kinesis: str = """
                                inputs:
                                  - type: kinesis-data-stream
                                    id: arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            config = parse_config(config_yaml_kinesis)
            event_input = config.get_input_by_id(
                "arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream"
            )
            assert event_input is not None
            shipper = get_shipper_from_input(
                event_input=event_input, lambda_event={}, at_record=0, config_yaml=config_yaml_kinesis
            )
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)
        with self.subTest("Logstash shipper from Cloudwatch logs input"):
            config_yaml_cw: str = """
                                inputs:
                                  - type: cloudwatch-logs
                                    id: arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs
                                    outputs:
                                        - type: logstash
                                          args:
                                            logstash_url: logstash_url
                            """
            config = parse_config(config_yaml_cw)
            event_input = config.get_input_by_id("arn:aws:logs:eu-central-1:123456789:stream/test-cw-logs")
            assert event_input is not None
            shipper = get_shipper_from_input(
                event_input=event_input, lambda_event={}, at_record=0, config_yaml=config_yaml_cw
            )
            assert len(shipper._shippers) == 1
            assert isinstance(shipper._shippers[0], LogstashShipper)
