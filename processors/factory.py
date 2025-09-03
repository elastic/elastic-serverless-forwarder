from typing import Any, Dict, List, Optional

from .processor import BaseProcessor, ProcessorResult
from .registry import ProcessorRegistry


class ProcessorChain:
    """
    Chain of processors to process events.
    """

    def __init__(self, processors: List[BaseProcessor]) -> None:
        self.processors = processors

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:

        if context is None:
            context = {}

        result = ProcessorResult()
        for processor in self.processors:
            result = processor.process(event, context)
            if result.is_empty:
                break

        return result


class ProcessorFactory:
    """
    Factory for creating processor instances and processor chains.
    """

    @staticmethod
    def create(processor_type: str, **kwargs: Any) -> BaseProcessor:

        processor_class = ProcessorRegistry.get(processor_type)
        processor = processor_class()
        processor.configure(kwargs)
        return processor

    @staticmethod
    def create_chain(processor_configs: List[Dict[str, Any]]) -> ProcessorChain:
        """
        Create a processor chain from a list of processor configurations.

        Args:
            processor_configs: List of processor configurations, each containing
                             a 'type' field and optional configuration fields

        Returns:
            A processor chain

        Raises:
            ValueError: If any processor type is unknown or configuration is invalid

        Example:
            configs = [
                {"type": "filter", "filter_field": "level", "filter_values": ["ERROR"]},
                {"type": "add_field", "fields": {"processed": True}}
            ]
            chain = ProcessorFactory.create_chain(configs)
        """
        processors = []

        for config in processor_configs:
            if "type" not in config:
                raise ValueError("Processor configuration must include 'type' field")

            processor_type = config.pop("type")
            processor = ProcessorFactory.create(processor_type, **config)
            processors.append(processor)

        return ProcessorChain(processors)
