from ipaddress import IPv4Address
import unittest
import struct
import gzip
from io import BytesIO
from unittest.mock import Mock, patch
import json

from share import parse_ipfix_stream
from processors.ipfix_ecs import ECSProcessor
from storage.s3 import S3Storage


class TestIPFIXECSIntegration(unittest.TestCase):
    """Test the integration of IPFIX processor with ECS processor."""

    def setUp(self):
        """Set up test fixtures."""
        self.ecs_processor = ECSProcessor()

    def create_simple_ipfix_stream(self) -> BytesIO:
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
        return BytesIO(ipfix_data)

    def test_ipfix_to_ecs_pipeline(self):
        """Test that IPFIX data can be processed and converted to ECS format."""
        # Create a simple IPFIX message with template and data
        ipfix_data = self.create_simple_ipfix_stream()

        ipfix_result = parse_ipfix_stream(ipfix_data)

        # Get the first IPFIX record
        for event in ipfix_result:
            ipfix_record = event
            break

        # Verify IPFIX record has expected fields
        self.assertIn('sourceIPv4Address', ipfix_record)
        self.assertIn('destinationIPv4Address', ipfix_record)

        # Process through ECS processor
        ecs_result = self.ecs_processor.process(ipfix_record)

        self.assertFalse(ecs_result.is_empty, "ECS processor should produce results")
        self.assertEqual(len(ecs_result.events), 1, "Should have exactly one ECS record")

        # Verify ECS structure
        ecs_record = ecs_result.events[0]

        # Check required ECS fields
        self.assertIn('event', ecs_record)
        self.assertIn('source', ecs_record)
        self.assertIn('destination', ecs_record)
        self.assertIn('network', ecs_record)
        self.assertIn('netflow', ecs_record)

        # Check source and destination IPs
        self.assertEqual(IPv4Address(int(ecs_record['source']['ip'], 16)), IPv4Address('192.168.1.1'))
        self.assertEqual(IPv4Address(int(ecs_record['destination']['ip'], 16)), IPv4Address('10.0.0.1'))

        # Check event metadata
        self.assertEqual(ecs_record['event']['kind'], 'event')
        self.assertIn('network', ecs_record['event']['category'])

    def test_simple_ipfix_ecs_pipeline(self):
        """Test a simplified IPFIX to ECS pipeline."""
        # Create sample IPFIX-like data (simplified for testing)
        sample_ipfix_record = {
            'sourceIPv4Address': '192.168.1.100',
            'destinationIPv4Address': '10.0.0.50',
            'sourceTransportPort': 443,
            'destinationTransportPort': 80,
            'protocolIdentifier': 6,
            'octetDeltaCount': 1024,
            '@timestamp': 1640995200
        }

        # Process through ECS processor
        ecs_result = self.ecs_processor.process(sample_ipfix_record)

        self.assertFalse(ecs_result.is_empty, "ECS processor should produce results")
        self.assertEqual(len(ecs_result.events), 1, "Should have exactly one ECS record")

        # Verify ECS structure
        ecs_record = ecs_result.events[0]

        # Check required ECS fields
        self.assertIn('event', ecs_record)
        self.assertIn('source', ecs_record)
        self.assertIn('destination', ecs_record)
        self.assertIn('network', ecs_record)
        self.assertIn('netflow', ecs_record)

        # Check source and destination IPs
        self.assertEqual(ecs_record['source']['ip'], '192.168.1.100')
        self.assertEqual(ecs_record['destination']['ip'], '10.0.0.50')

        # Check event metadata
        self.assertEqual(ecs_record['event']['kind'], 'event')
        self.assertIn('network', ecs_record['event']['category'])

    @patch('storage.s3.boto3')
    def test_s3_storage_ipfix_ecs_processing(self, mock_boto3):
        """Test that S3 storage processes IPFIX files through both processors."""
        # Create a simple IPFIX message
        ipfix_data: bytes = b''

        # Compress the data
        compressed_data = self.create_simple_ipfix_stream()
        with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz:
            gz.write(ipfix_data)
        compressed_data.seek(0)

        # Mock S3 client
        mock_s3_client = Mock()
        mock_s3_client.head_object.return_value = {
            'ContentType': 'application/gzip',
            'ContentLength': len(compressed_data.getvalue())
        }

        def mock_download(bucket, key, fileobj):
            fileobj.write(compressed_data.getvalue())

        mock_s3_client.download_fileobj.side_effect = mock_download
        mock_boto3.client.return_value = mock_s3_client

        # Create S3 storage instance
        storage = S3Storage("test-bucket", "test-key.gz")

        # Process with binary_processor_type="ipfix"
        results = list(storage.get_by_lines(0))

        self.assertGreater(len(results), 0, "Should produce results from IPFIX+ECS processing")

        # Parse the first result
        json_bytes, start_offset, end_offset, event_offset = results[0]
        result_data = json.loads(json_bytes.decode('utf-8'))

        # Verify it's in ECS format
        self.assertIn('event', result_data)
        self.assertIn('source', result_data)
        self.assertIn('destination', result_data)
        self.assertIn('network', result_data)
        self.assertIn('netflow', result_data)

        # Verify the source and destination IPs were processed correctly
        self.assertEqual(result_data['source']['ip'], '192.168.1.1')
        self.assertEqual(result_data['destination']['ip'], '10.0.0.1')

    def test_ecs_processor_registration(self):
        """Test that the ECS processor is registered correctly."""
        from processors.registry import ProcessorRegistry

        # The processor should be registered
        processor_class = ProcessorRegistry.get("ipfix_ecs")
        self.assertEqual(processor_class, ECSProcessor)

        # Should be able to create an instance
        processor = processor_class()
        self.assertIsInstance(processor, ECSProcessor)


if __name__ == '__main__':
    unittest.main()
