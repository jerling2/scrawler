from source.broker.interfaces import MessageInterface
from typing import Any, Protocol


class APIProtocol(Protocol):
    source_topic: str
    model: MessageInterface

    def get_payload(self, *args, **kwargs) -> Any:
        ...    
