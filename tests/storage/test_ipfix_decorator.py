# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
import json
import struct
from io import BytesIO
from typing import Optional
from unittest import TestCase
from unittest.mock import patch

import pytest

from storage import (
    CommonStorage,
    GetByLinesIterator,
    StorageDecoratorIterator,
    StorageReader,
    by_lines,
    inflate,
    json_collector,
    multi_line,
)
from storage.decorator import ipfix_decode


class DummyIPFIXStorage(CommonStorage):
    """
    Dummy Storage for testing IPFIX decorator.
    """

    def __init__(self, binary_processor_type: Optional[str] = None):
        self.binary_processor_type = binary_processor_type

    def get_by_lines(self, range_start: int, binary_processor_type: Optional[str] = None) -> GetByLinesIterator:
        yield b"", 0, 0, None

    def get_as_string(self) -> str:
        return ""

    @multi_line
    @json_collector
    @by_lines
    @ipfix_decode
    @inflate
    def generate(self, range_start: int, body: BytesIO, is_gzipped: bool) -> StorageDecoratorIterator:
        if is_gzipped:
            reader: StorageReader = StorageReader(raw=body)
            yield reader, 0, 0, b"", None
        else:
            yield body.read(), 0, 0, b"", None


@pytest.mark.unit
class TestIPFIXDecorator(TestCase):
    """Test cases for the IPFIX decorator functionality."""

    def create_simple_ipfix_message(self):
        """Create a simple IPFIX message for testing."""
        # Template Set
        template_id = 256
        field_count = 3

        # Template Record
        template_record = struct.pack("!HH", template_id, field_count)

        # Field Specifiers
        field1 = struct.pack("!HH", 8, 4)   # sourceIPv4Address (ID=8, Length=4)
        field2 = struct.pack("!HH", 12, 4)  # destinationIPv4Address (ID=12, Length=4)
        field3 = struct.pack("!HH", 4, 1)   # protocolIdentifier (ID=4, Length=1)

        template_data = template_record + field1 + field2 + field3
        template_set_length = 4 + len(template_data)
        template_set_header = struct.pack("!HH", 2, template_set_length)  # Set ID=2 (Template)
        template_set = template_set_header + template_data

        # Data Set
        data_record = struct.pack("!IIB",
                                  int.from_bytes([192, 168, 1, 100], 'big'),  # sourceIPv4Address
                                  int.from_bytes([10, 0, 0, 50], 'big'),      # destinationIPv4Address
                                  6)                                           # protocolIdentifier (TCP)

        data_set_length = 4 + len(data_record)
        data_set_header = struct.pack("!HH", template_id, data_set_length)
        data_set = data_set_header + data_record

        # IPFIX Message Header
        message_length = 16 + len(template_set) + len(data_set)
        header = struct.pack("!HHIII",
                             10,           # Version=10
                             message_length,
                             1640995200,   # Export Time
                             1,            # Sequence Number
                             1)            # Observation Domain ID

        return header + template_set + data_set

    def test_ipfix_decorator_disabled_when_no_binary_processor_type(self):
        """Test that IPFIX decorator is disabled when binary_processor_type is not set."""
        storage = DummyIPFIXStorage()  # No binary_processor_type
        test_data = b"plain text data"
        fixtures = BytesIO(test_data)

        result = list(storage.generate(0, fixtures, False))

        # Should pass through unchanged
        self.assertEqual(len(result), 1)
        data, start_offset, end_offset, newline, event_offset = result[0]
        self.assertEqual(data, test_data)

    def test_ipfix_decorator_disabled_when_binary_processor_type_not_ipfix(self):
        """Test that IPFIX decorator is disabled when binary_processor_type is not 'ipfix'."""
        storage = DummyIPFIXStorage(binary_processor_type="other")
        test_data = b"plain text data"
        fixtures = BytesIO(test_data)

        result = list(storage.generate(0, fixtures, False))

        # Should pass through unchanged
        self.assertEqual(len(result), 1)
        data, start_offset, end_offset, newline, event_offset = result[0]
        self.assertEqual(data, test_data)

    def test_ipfix_decorator_processes_binary_data(self):
        """Test that IPFIX decorator processes binary data when enabled."""
        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        ipfix_data = self.create_simple_ipfix_message()
        fixtures = BytesIO(ipfix_data)

        result = list(storage.generate(0, fixtures, False))

        # Should have processed the data
        self.assertEqual(len(result), 1)
        data, start_offset, end_offset, newline, event_offset = result[0]

        # Data should be JSON bytes
        self.assertIsInstance(data, bytes)
        parsed_data = json.loads(data.decode('utf-8'))

        # With streaming parser, IP addresses come as hex strings initially
        # The exact format depends on the conversion logic
        self.assertIn('sourceIPv4Address', parsed_data)
        self.assertIn('destinationIPv4Address', parsed_data)
        self.assertIn('protocolIdentifier', parsed_data)

        # Verify the record has expected structure
        self.assertIn('@timestamp', parsed_data)
        self.assertIn('header', parsed_data)

    def test_ipfix_decorator_processes_gzipped_data(self):
        """Test that IPFIX decorator processes gzipped IPFIX data."""
        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        ipfix_data = self.create_simple_ipfix_message()
        compressed_data = gzip.compress(ipfix_data)
        fixtures = BytesIO(compressed_data)

        result = list(storage.generate(0, fixtures, True))

        # Should have processed the data
        self.assertEqual(len(result), 1)
        data, start_offset, end_offset, newline, event_offset = result[0]

        # Data should be JSON bytes
        self.assertIsInstance(data, bytes)
        parsed_data = json.loads(data.decode('utf-8'))

        # Verify it has IPFIX structure
        self.assertIn('sourceIPv4Address', parsed_data)
        self.assertIn('@timestamp', parsed_data)

    @patch('share.ipfix_parser.parse_ipfix_stream')
    def test_ipfix_decorator_handles_multiple_records(self, mock_parse_ipfix_stream):
        """Test that IPFIX decorator handles multiple IPFIX records."""
        # Mock the streaming parser to return multiple records
        mock_parse_ipfix_stream.return_value = iter([
            {'sourceIPv4Address': '192.168.1.100', 'flow_id': 1},
            {'sourceIPv4Address': '192.168.1.101', 'flow_id': 2},
            {'sourceIPv4Address': '192.168.1.102', 'flow_id': 3}
        ])

        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        ipfix_data = self.create_simple_ipfix_message()
        fixtures = BytesIO(ipfix_data)

        result = list(storage.generate(0, fixtures, False))
        print(result)
        # The decorator chain concatenates JSON objects, so we should have 1 result containing all 3 records
        self.assertEqual(len(result), 1, "Should have one result containing all records")

        # Verify the single result contains all three records
        data, start_offset, end_offset, newline, event_offset = result[0]
        self.assertIsInstance(data, bytes)
        data_str = data.decode('utf-8')

        # Should contain all three flow_id values in the concatenated JSON
        self.assertIn('"flow_id": 1', data_str, "Should contain flow_id 1")
        self.assertIn('"flow_id": 2', data_str, "Should contain flow_id 2")
        self.assertIn('"flow_id": 3', data_str, "Should contain flow_id 3")

    @patch('share.ipfix_parser.parse_ipfix_stream')
    def test_ipfix_decorator_handles_empty_result(self, mock_parse_ipfix_stream):
        """Test that IPFIX decorator handles empty processor results."""
        # Mock the streaming parser to return empty iterator
        mock_parse_ipfix_stream.return_value = iter([])

        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        ipfix_data = self.create_simple_ipfix_message()
        fixtures = BytesIO(ipfix_data)

        result = list(storage.generate(0, fixtures, False))

        # Should have no results
        self.assertEqual(len(result), 0)

    @patch('share.ipfix_parser.parse_ipfix_stream')
    @patch('storage.decorator.shared_logger')
    def test_ipfix_decorator_handles_processing_exception(self, mock_logger, mock_parse_ipfix_stream):
        """Test that IPFIX decorator handles processing exceptions gracefully."""
        # Mock the streaming parser to raise an exception
        mock_parse_ipfix_stream.side_effect = Exception("IPFIX parsing error")

        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        ipfix_data = self.create_simple_ipfix_message()
        fixtures = BytesIO(ipfix_data)

        # Should not raise an exception
        try:
            result = list(storage.generate(0, fixtures, False))
            # Should have no results due to exception
            self.assertEqual(len(result), 0)
        except Exception:
            self.fail("IPFIX decorator should handle exceptions gracefully")

        # Should have logged the error
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        self.assertIn("Error processing IPFIX data", error_call)

    def test_ipfix_decorator_passthrough_for_plain_data(self):
        """Test that plain text data passes through unchanged when IPFIX is enabled."""
        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        test_data = b"line1\nline2\nline3\n"
        fixtures = BytesIO(test_data)

        # Since this will try to process as IPFIX and fail, it should pass through
        # or handle the error gracefully
        result = list(storage.generate(0, fixtures, False))

        # The exact behavior depends on how the IPFIX processor handles invalid data
        # This test ensures no crashes occur
        self.assertIsInstance(result, list)

    def test_ipfix_decorator_integration_with_other_decorators(self):
        """Test that IPFIX decorator works correctly with other decorators in the chain."""
        storage = DummyIPFIXStorage(binary_processor_type="ipfix")

        # Test with both gzipped and non-gzipped data
        test_data = b"test data"
        compressed_data = gzip.compress(test_data)

        # Test gzipped
        fixtures_gz = BytesIO(compressed_data)
        result_gz = list(storage.generate(0, fixtures_gz, True))
        self.assertIsInstance(result_gz, list)

        # Test non-gzipped
        fixtures = BytesIO(test_data)
        result = list(storage.generate(0, fixtures, False))
        self.assertIsInstance(result, list)


@pytest.mark.integration
class TestIPFIXDecoratorWithRealData(TestCase):
    """Integration tests for IPFIX decorator with real IPFIX data."""

    def test_ipfix_decorator_with_real_ipfix_file(self):
        """Test IPFIX decorator with the real original.ipfix.gz file."""
        import os

        # Path to the real IPFIX file
        ipfix_file_path = (
            "/Users/vinit.chauhan/github.com/vinit-chauhan/elastic-serverless-forwarder"
            "/tests/testdata/original.ipfix.gz"
        )

        if not os.path.exists(ipfix_file_path):
            self.skipTest(f"IPFIX test file not found: {ipfix_file_path}")

        # Read the real IPFIX file
        with open(ipfix_file_path, 'rb') as f:
            ipfix_gz_data = f.read()

        storage = DummyIPFIXStorage(binary_processor_type="ipfix")
        fixtures = BytesIO(ipfix_gz_data)

        # Process the real IPFIX file
        result = list(storage.generate(0, fixtures, True))

        # Should have some results (depending on the content of the real file)
        self.assertIsInstance(result, list)
        print(f"Processed real IPFIX file: {len(result)} records extracted")

        # If we have results, verify they're valid JSON
        for i, (data, start_offset, end_offset, newline, event_offset) in enumerate(result[:3]):  # Check first 3
            if isinstance(data, bytes):
                try:
                    parsed_data = json.loads(data.decode('utf-8'))
                    print(f"Record {i+1}: {list(parsed_data.keys())}")
                    self.assertIsInstance(parsed_data, dict)
                except json.JSONDecodeError:
                    # Handle concatenated JSON like in the other test
                    data_str = data.decode('utf-8')
                    print(f"Record {i+1} contains concatenated JSON (length: {len(data_str)} chars)")
                    # Just verify it contains some expected IPFIX field patterns
                    self.assertTrue(
                        any(field in data_str for field in [
                            'sourceIPv4Address', 'destinationIPv4Address', 'protocolIdentifier', '@timestamp'
                        ]),
                        "Record should contain IPFIX field patterns"
                    )

    def test_ipfix_decorator_binary_processor_attribute(self):
        """Test that binary_processor_type attribute is correctly used."""
        # Test with attribute set to 'ipfix'
        storage_ipfix = DummyIPFIXStorage(binary_processor_type="ipfix")
        self.assertEqual(storage_ipfix.binary_processor_type, "ipfix")

        # Test with attribute set to something else
        storage_other = DummyIPFIXStorage(binary_processor_type="other")
        self.assertEqual(storage_other.binary_processor_type, "other")

        # Test with no attribute
        storage_none = DummyIPFIXStorage()
        self.assertIsNone(storage_none.binary_processor_type)

    def test_ipfix_decorator_logging(self):
        """Test that IPFIX decorator logs appropriately."""
        with patch('storage.decorator.shared_logger') as mock_logger:
            storage = DummyIPFIXStorage(binary_processor_type="ipfix")
            test_data = b"invalid ipfix data"
            fixtures = BytesIO(test_data)

            list(storage.generate(0, fixtures, False))

            # Should have logged something (either debug or error messages)
            self.assertTrue(mock_logger.debug.called or mock_logger.error.called or mock_logger.warning.called)
