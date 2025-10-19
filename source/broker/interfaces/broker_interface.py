from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class MessageModel:
    serialize: Callable[[Any], bytes]
    deserialize: Callable[[bytes], Any]


@dataclass(frozen=True)
class ConsumerConfig:
    topics: list[str]
    model: MessageModel
    handler: Callable[[Any], None]
