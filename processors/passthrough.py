from typing import Dict, Any, Optional

from processors.processor import BaseProcessor, ProcessorResult


class PassThroughProcessor(BaseProcessor):
    """
    Passthrough processor for initial testing
    """

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        return ProcessorResult(event)
