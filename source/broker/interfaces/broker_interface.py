from dataclasses import dataclass
from typing import Any, Callable, Protocol


class MessageInterface(Protocol):
    def serialize(self, payload: Any) -> bytes:
        ...

    def deserialize(self, data: bytes) -> Any:
        ...


@dataclass(frozen=True)
class ConsumerConfig:
    topics: list[str]
    model: MessageInterface
    notify: Callable[[Any], None]
