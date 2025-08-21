
from typing import Callable, Dict, List, Type

from processors.processor import BaseProcessor


class ProcessorRegistry:
    _processors: Dict[str, Type[BaseProcessor]] = {}

    @classmethod
    def register(cls, processor_type: str, processor: Type[BaseProcessor]) -> None:
        cls._processors[processor_type] = processor

    @classmethod
    def get(cls, processor_type: str) -> Type[BaseProcessor]:
        if processor_type not in cls._processors:
            raise ValueError(f"Processor type '{processor_type}' is not registered.")

        return cls._processors[processor_type]

    @classmethod
    def get_available_types(cls) -> List[str]:
        return list(cls._processors.keys())


def register_processor(processor_type: str) -> Callable[[Type[BaseProcessor]], Type[BaseProcessor]]:

    def decorator(processor_class: Type[BaseProcessor]) -> Type[BaseProcessor]:
        ProcessorRegistry.register(processor_type, processor_class)
        return processor_class

    return decorator
