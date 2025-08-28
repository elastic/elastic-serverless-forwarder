# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Tests for IPFIX processor
"""

import struct
import gzip
from unittest import TestCase

import pytest

from processors.ipfix import IPFIXProcessor, IPFIXReader, TemplateSet, DataRecord
from processors.factory import ProcessorFactory
from processors.processor import ProcessorResult


class TestIPFIXProcessor(TestCase):
    """Test the IPFIX processor"""

    def setUp(self):
        """Set up test fixtures"""
        self.processor = IPFIXProcessor()

    def create_simple_ipfix_file(self):
        """Create a simple IPFIX file for testing"""
        # Template Set
        template_id = 256
        field_count = 2

        # Template Record: Template ID + Field Count + Field Specifiers
        template_record = struct.pack("!HH", template_id, field_count)

        # Field 1: sourceIPv4Address (ID=8, Length=4)
        field1 = struct.pack("!HH", 8, 4)

        # Field 2: destinationIPv4Address (ID=12, Length=4)
        field2 = struct.pack("!HH", 12, 4)

        template_data = template_record + field1 + field2
        template_set_length = 4 + len(template_data)  # 4 bytes for set header
        template_set_header = struct.pack("!HH", 2, template_set_length)  # Set ID=2 (Template)
        template_set = template_set_header + template_data

        # Data Set - Set ID=256 (matching template ID)
        # Data Record: sourceIP=192.168.1.1, destIP=10.0.0.1
        data_record = struct.pack("!II",
                                  int.from_bytes([192, 168, 1, 1], 'big'),  # sourceIPv4Address
                                  int.from_bytes([10, 0, 0, 1], 'big'))     # destinationIPv4Address

        data_set_length = 4 + len(data_record)  # 4 bytes for set header
        data_set_header = struct.pack("!HH", template_id, data_set_length)  # Set ID=256
        data_set = data_set_header + data_record

        # IPFIX Message Header
        message_length = 16 + len(template_set) + len(data_set)  # 16 bytes for message header
        header = struct.pack("!HHIII",
                             10,           # Version=10
                             message_length,  # Length
                             1640995200,   # Export Time
                             1,            # Sequence Number
                             1)            # Observation Domain ID

        # Combine all parts
        ipfix_data = header + template_set + data_set
        return ipfix_data

    def create_multi_record_ipfix_file(self, compressed=False):
        """Create a realistic IPFIX file with multiple records"""
        # Template Set
        template_id = 256
        field_count = 6

        template_record = struct.pack("!HH", template_id, field_count)

        # Fields: sourceIPv4Address, destinationIPv4Address, sourceTransportPort,
        # destinationTransportPort, protocolIdentifier, octetDeltaCount
        fields = [
            struct.pack("!HH", 8, 4),   # sourceIPv4Address
            struct.pack("!HH", 12, 4),  # destinationIPv4Address
            struct.pack("!HH", 7, 2),   # sourceTransportPort
            struct.pack("!HH", 11, 2),  # destinationTransportPort
            struct.pack("!HH", 4, 1),   # protocolIdentifier
            struct.pack("!HH", 1, 8),   # octetDeltaCount
        ]

        template_data = template_record + b''.join(fields)
        template_set_length = 4 + len(template_data)
        template_set_header = struct.pack("!HH", 2, template_set_length)
        template_set = template_set_header + template_data

        # Data Set with multiple records
        data_records = []

        # Record 1: 192.168.1.1:80 -> 10.0.0.1:443, TCP, 1024 bytes
        data_records.append(
            struct.pack("!I", int.from_bytes([192, 168, 1, 1], 'big')) +
            struct.pack("!I", int.from_bytes([10, 0, 0, 1], 'big')) +
            struct.pack("!H", 80) +
            struct.pack("!H", 443) +
            struct.pack("!B", 6) +
            struct.pack("!Q", 1024)
        )

        # Record 2: 192.168.1.2:53 -> 10.0.0.2:35234, UDP, 256 bytes
        data_records.append(
            struct.pack("!I", int.from_bytes([192, 168, 1, 2], 'big')) +
            struct.pack("!I", int.from_bytes([10, 0, 0, 2], 'big')) +
            struct.pack("!H", 53) +
            struct.pack("!H", 35234) +
            struct.pack("!B", 17) +
            struct.pack("!Q", 256)
        )

        data_set_data = b''.join(data_records)
        data_set_length = 4 + len(data_set_data)
        data_set_header = struct.pack("!HH", template_id, data_set_length)
        data_set = data_set_header + data_set_data

        # IPFIX Message Header
        message_length = 16 + len(template_set) + len(data_set)
        header = struct.pack("!HHIII",
                             10,           # Version
                             message_length,  # Length
                             1640995200,   # Export Time
                             1,            # Sequence Number
                             1)            # Observation Domain ID

        ipfix_data = header + template_set + data_set

        if compressed:
            # Compress the data
            compressed_data = gzip.compress(ipfix_data)
            return compressed_data

        return ipfix_data

    @pytest.mark.unit
    def test_ipfix_processor_basic(self):
        """Test basic IPFIX processor functionality"""
        # Create test IPFIX data
        ipfix_data = self.create_simple_ipfix_file()

        # Test processor
        event = {"body": ipfix_data}
        result = self.processor.process(event)

        self.assertIsInstance(result, ProcessorResult)
        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 1)

        # Check the processed event
        processed_event = result.events[0]
        self.assertIn('sourceIPv4Address', processed_event)
        self.assertIn('destinationIPv4Address', processed_event)
        self.assertIn('processor', processed_event)
        self.assertEqual(processed_event['processor']['type'], 'ipfix')

    @pytest.mark.unit
    def test_ipfix_processor_multiple_records(self):
        """Test IPFIX processor with multiple records"""
        # Create test IPFIX data with multiple records
        ipfix_data = self.create_multi_record_ipfix_file()

        # Test processor
        event = {"body": ipfix_data}
        result = self.processor.process(event)

        self.assertIsInstance(result, ProcessorResult)
        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 2)

        # Check first record
        record1 = result.events[0]
        self.assertEqual(record1['sourceIPv4Address'], '192.168.1.1')
        self.assertEqual(record1['destinationIPv4Address'], '10.0.0.1')
        self.assertEqual(record1['sourceTransportPort'], 80)
        self.assertEqual(record1['destinationTransportPort'], 443)
        self.assertEqual(record1['protocolIdentifier'], 6)  # TCP

        # Check second record
        record2 = result.events[1]
        self.assertEqual(record2['sourceIPv4Address'], '192.168.1.2')
        self.assertEqual(record2['destinationIPv4Address'], '10.0.0.2')
        self.assertEqual(record2['sourceTransportPort'], 53)
        self.assertEqual(record2['destinationTransportPort'], 35234)
        self.assertEqual(record2['protocolIdentifier'], 17)  # UDP

    @pytest.mark.unit
    def test_ipfix_processor_with_context(self):
        """Test IPFIX processor with context"""
        ipfix_data = self.create_simple_ipfix_file()

        event = {"body": ipfix_data}
        context = {"input_id": "test-input", "input_type": "s3-sqs"}
        result = self.processor.process(event, context)

        self.assertFalse(result.is_empty)
        processed_event = result.events[0]
        self.assertEqual(processed_event['input_id'], 'test-input')
        self.assertEqual(processed_event['input_type'], 's3-sqs')

    @pytest.mark.unit
    def test_ipfix_processor_empty_data(self):
        """Test IPFIX processor with no data"""
        event = {}
        result = self.processor.process(event)

        self.assertIsInstance(result, ProcessorResult)
        self.assertTrue(result.is_empty)

    @pytest.mark.unit
    def test_ipfix_processor_invalid_data(self):
        """Test IPFIX processor with invalid data"""
        event = {"body": b"invalid ipfix data"}
        result = self.processor.process(event)

        self.assertIsInstance(result, ProcessorResult)
        self.assertTrue(result.is_empty)

    @pytest.mark.unit
    def test_ipfix_reader_basic(self):
        """Test IPFIX reader directly"""
        ipfix_data = self.create_simple_ipfix_file()

        reader = IPFIXReader(file_bytes=ipfix_data)

        records = []
        while True:
            record = reader.next()
            if record is None:
                break
            records.append(record.get_data())

        reader.close()

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record['sourceIPv4Address'], '192.168.1.1')
        self.assertEqual(record['destinationIPv4Address'], '10.0.0.1')

    @pytest.mark.unit
    def test_processor_factory_registration(self):
        """Test that IPFIX processor is properly registered"""
        processor = ProcessorFactory.create("ipfix")
        self.assertIsInstance(processor, IPFIXProcessor)

    @pytest.mark.unit
    def test_template_set_parsing(self):
        """Test template set parsing"""
        # Create template data
        template_data = struct.pack("!HH", 256, 2)  # Template ID=256, Field Count=2
        template_data += struct.pack("!HH", 8, 4)   # sourceIPv4Address
        template_data += struct.pack("!HH", 12, 4)  # destinationIPv4Address

        template = TemplateSet.parse_from_data(template_data)

        self.assertEqual(template.template_id, 256)
        fields = template.get_fields()
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0][0], 'sourceIPv4Address')
        self.assertEqual(fields[1][0], 'destinationIPv4Address')

    @pytest.mark.unit
    def test_data_record_parsing(self):
        """Test data record parsing"""
        # Create template
        template = TemplateSet(256)
        template.add_field('sourceIPv4Address', 4, 'ipv4Address')
        template.add_field('destinationIPv4Address', 4, 'ipv4Address')

        # Create data
        data = struct.pack("!II",
                           int.from_bytes([192, 168, 1, 1], 'big'),
                           int.from_bytes([10, 0, 0, 1], 'big'))

        msg_header = {'export_time': 1640995200}
        record = DataRecord.parse_from_template(template, data, msg_header)

        self.assertIsNotNone(record)
        record_data = record.get_data()
        self.assertEqual(record_data['sourceIPv4Address'], '192.168.1.1')
        self.assertEqual(record_data['destinationIPv4Address'], '10.0.0.1')


if __name__ == "__main__":
    pytest.main([__file__])
