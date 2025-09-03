# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Tests for the Processor System used in S3 Binary Processing Flow
"""

from unittest import TestCase
from unittest.mock import patch
from typing import Any, Dict, Optional

import pytest

from processors.factory import ProcessorChain, ProcessorFactory
from processors.passthrough import PassThroughProcessor
from processors.processor import BaseProcessor, ProcessorResult
from processors.utils import process_event


class MockProcessor(BaseProcessor):
    """Mock processor for testing"""

    def __init__(self):
        super().__init__()
        self.processed_events = []

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        # Add a test field to track processing
        processed_event = event.copy()
        processed_event['processed_by_mock'] = True
        processed_event['processor_config'] = self._config

        self.processed_events.append(processed_event)
        return ProcessorResult(processed_event)


class TestProcessorSystem(TestCase):
    """Test the processor system components"""

    def setUp(self):
        """Set up test environment"""
        self.sample_event = {
            "@timestamp": "2025-01-26T10:30:00Z",
            "fields": {
                "message": '{"timestamp": "2025-01-26T10:30:00Z", "level": "INFO", "message": "Test message"}',
                "log": {
                    "offset": 0,
                    "file": {"path": "s3://bucket/key"}
                },
                "aws": {
                    "s3": {
                        "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                        "object": {"key": "test-key"}
                    }
                }
            }
        }

    @pytest.mark.unit
    def test_processor_result_creation(self):
        """Test ProcessorResult creation and methods"""
        # Test empty result
        empty_result = ProcessorResult()
        self.assertTrue(empty_result.is_empty)
        self.assertEqual(len(empty_result), 0)
        self.assertEqual(empty_result.to_dict(), {})

        # Test single event result
        single_result = ProcessorResult(self.sample_event)
        self.assertFalse(single_result.is_empty)
        self.assertEqual(len(single_result), 1)
        self.assertEqual(single_result.to_dict(), self.sample_event)

        # Test multiple events result
        multiple_events = [self.sample_event, self.sample_event.copy()]
        multiple_result = ProcessorResult(multiple_events)
        self.assertFalse(multiple_result.is_empty)
        self.assertEqual(len(multiple_result), 2)
        result_dict = multiple_result.to_dict()
        self.assertIn('0', result_dict)
        self.assertIn('1', result_dict)

    @pytest.mark.unit
    def test_base_processor_configuration(self):
        """Test BaseProcessor configuration"""
        processor = MockProcessor()

        # Test initial state
        self.assertEqual(processor._config, {})

        # Test configuration
        config = {"test_param": "test_value", "number_param": 42}
        processor.configure(config)
        self.assertEqual(processor._config, config)

        # Test processing
        result = processor.process(self.sample_event)
        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 1)

        processed_event = result.to_dict()
        self.assertTrue(processed_event['processed_by_mock'])
        self.assertEqual(processed_event['processor_config'], config)

    @pytest.mark.unit
    def test_processor_factory_creation(self):
        """Test ProcessorFactory creation of processors"""
        # Mock processor registry
        with patch('processors.factory.ProcessorRegistry') as mock_registry:
            mock_registry.get.return_value = MockProcessor

            # Test processor creation
            processor = ProcessorFactory.create("mock", test_param="test_value")

            self.assertIsInstance(processor, MockProcessor)
            self.assertEqual(processor._config, {"test_param": "test_value"})
            mock_registry.get.assert_called_once_with("mock")

    @pytest.mark.unit
    def test_process_event_utility(self):
        """Test the process_event utility function"""
        # Test with None processor chain
        result = process_event(self.sample_event, None, {})
        self.assertIsInstance(result, ProcessorResult)
        self.assertEqual(result.to_dict(), self.sample_event)

        # Test with actual processor chain
        mock_proc = MockProcessor()
        mock_proc.configure({"test": "value"})
        chain = ProcessorChain([mock_proc])

        context = {"input_id": "test", "input_type": "s3-sqs"}
        result = process_event(self.sample_event, chain, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        self.assertTrue(processed_event['processed_by_mock'])

    @pytest.mark.unit
    def test_processor_factory_invalid_config(self):
        """Test ProcessorFactory with invalid configurations"""
        # Test missing type field
        with self.assertRaises(ValueError) as context:
            ProcessorFactory.create_chain([{"param": "value"}])

        self.assertIn("'type' field", str(context.exception))

    @pytest.mark.unit
    def test_passthrough_processor(self):
        """Test PassThroughProcessor behavior"""
        processor = PassThroughProcessor()
        processor.configure({})

        result = processor.process(self.sample_event)
        self.assertFalse(result.is_empty)
        self.assertEqual(result.to_dict(), self.sample_event)


if __name__ == "__main__":
    pytest.main([__file__])
