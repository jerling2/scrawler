from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeAlias


on_notify: TypeAlias = Callable[[Any], None]


class MessageInterface(Protocol):
    def serialize(self, payload: Any) -> bytes:
        ...

    def deserialize(self, data: bytes) -> Any:
        ...


@dataclass(frozen=True)
class IPGConsumerConfig:
    topics: list[str]
    model: MessageInterface
    notify: on_notify
