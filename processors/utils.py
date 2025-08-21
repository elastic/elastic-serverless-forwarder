from typing import Any, Dict, Optional

from share.logger import logger as shared_logger

from .processor import ProcessorResult
from .factory import ProcessorChain


def process_event(event: Dict[str, Any], processor_chain: Optional[ProcessorChain], context) -> ProcessorResult:

    if processor_chain is None:
        return ProcessorResult(event)

    if context is None:
        context = {}

    try:
        processed_event = processor_chain.process(event, context)
    except Exception as e:
        # Handle processing errors
        shared_logger.error("Error processing event", extra={"event": event, "error": str(e)})
        return ProcessorResult()

    return processed_event
