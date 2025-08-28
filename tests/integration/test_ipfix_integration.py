# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Integration tests for IPFIX processor with S3 storage
"""

import gzip
import struct
from unittest import TestCase

import pytest

from processors.ipfix import IPFIXProcessor
from share.config import parse_config


class TestIPFIXIntegration(TestCase):
    """Integration tests for IPFIX processing"""

    def create_test_ipfix_data(self):
        """Create test IPFIX data"""
        # Simple IPFIX message with template and data
        template_id = 256

        # Template Set
        template_record = struct.pack("!HH", template_id, 2)  # Template ID, Field Count
        template_record += struct.pack("!HH", 8, 4)   # sourceIPv4Address
        template_record += struct.pack("!HH", 12, 4)  # destinationIPv4Address

        template_set_length = 4 + len(template_record)
        template_set = struct.pack("!HH", 2, template_set_length) + template_record

        # Data Set
        data_record = struct.pack("!II",
                                  int.from_bytes([192, 168, 1, 1], 'big'),
                                  int.from_bytes([10, 0, 0, 1], 'big'))

        data_set_length = 4 + len(data_record)
        data_set = struct.pack("!HH", template_id, data_set_length) + data_record

        # IPFIX Message Header
        message_length = 16 + len(template_set) + len(data_set)
        header = struct.pack("!HHIII", 10, message_length, 1640995200, 1, 1)

        return header + template_set + data_set

    @pytest.mark.integration
    def test_ipfix_processor_end_to_end(self):
        """Test IPFIX processor end-to-end"""
        # Create test IPFIX data
        ipfix_data = self.create_test_ipfix_data()

        # Test processor directly
        processor = IPFIXProcessor()
        event = {"body": ipfix_data}

        result = processor.process(event)

        # Verify result
        self.assertIsNotNone(result)
        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 1)

        # Check that IPFIX data was processed
        processed_event = result.events[0]
        self.assertIn('sourceIPv4Address', processed_event)
        self.assertIn('destinationIPv4Address', processed_event)
        self.assertEqual(processed_event['sourceIPv4Address'], '192.168.1.1')
        self.assertEqual(processed_event['destinationIPv4Address'], '10.0.0.1')

        # Check processor metadata
        self.assertIn('processor', processed_event)
        self.assertEqual(processed_event['processor']['type'], 'ipfix')

    @pytest.mark.integration
    def test_ipfix_processor_with_gzip(self):
        """Test IPFIX processor with gzipped data"""
        # Create test IPFIX data
        ipfix_data = self.create_test_ipfix_data()
        compressed_data = gzip.compress(ipfix_data)

        # Test processor with compressed data
        processor = IPFIXProcessor()
        event = {"body": compressed_data}

        result = processor.process(event)

        # Should fail as processor expects decompressed data
        # (Storage layer handles decompression)
        self.assertTrue(result.is_empty)

    @pytest.mark.integration
    def test_config_validation(self):
        """Test that IPFIX configuration is valid"""
        config_yaml = """
inputs:
  - id: ipfix-input
    type: s3-sqs
    json_content_type: disabled
    binary_processor_type: ipfix
    args:
      queue_url: https://sqs.region.amazonaws.com/account/queue
    processors:
      - name: passthrough
        type: passthrough
    outputs:
      - type: elasticsearch
        args:
          elasticsearch_url: https://es.example.com
          username: user
          password: pass
"""

        # This should not raise an exception
        config = parse_config(config_yaml)
        input_config = config.get_input_by_id('ipfix-input')

        self.assertEqual(input_config.id, 'ipfix-input')
        self.assertEqual(input_config.type, 's3-sqs')
        self.assertEqual(input_config.binary_processor_type, 'ipfix')


if __name__ == "__main__":
    pytest.main([__file__])
