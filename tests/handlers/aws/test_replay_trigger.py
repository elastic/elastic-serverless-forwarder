# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest

from handlers.aws.replay_trigger import ReplayedEventReplayHandler, get_shipper_for_replay_event
from share import parse_config
from shippers.logstash import LogstashShipper


@pytest.mark.unit
class TestReplayTrigger(TestCase):
    def test_get_shipper_for_replay_event(self) -> None:
        with self.subTest("Logstash shipper from replay event"):
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
            replay_handler = ReplayedEventReplayHandler("arn:aws:sqs:eu-central-1:123456789:queue/replayqueue")
            logstash_shipper = get_shipper_for_replay_event(
                config,
                "logstash",
                {},
                "arn:aws:kinesis:eu-central-1:123456789:stream/test-esf-kinesis-stream",
                replay_handler,
            )
            assert isinstance(logstash_shipper, LogstashShipper)
