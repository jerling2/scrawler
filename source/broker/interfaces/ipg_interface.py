from typing import Protocol, Any, Callable
from dataclasses import dataclass


class IPGProtocol(Protocol):

    def serialize(self, payload: Any) -> bytes:
        ...
    
    def deserialize(self, payload: bytes) -> Any:
        ...


class IPGConsumerI(Protocol):
    topics: list[str]
    codec: IPGProtocol
    notify: Callable[[Any], None]


@dataclass(frozen=True)
class IPGConsumer:
    topics: list[str]
    codec: IPGProtocol
    notify: Callable[[Any], None]