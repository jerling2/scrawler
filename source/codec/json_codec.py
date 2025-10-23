from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class JsonCodec:
    
    payload: Any

    @classmethod
    def serialize(cls, message: JsonCodec) -> bytes:
        return json.dumps(message.payload).encode('utf-8')
    
    @classmethod
    def deserialize(cls, message: bytes) -> JsonCodec:
        return cls(json.loads(message.decode('utf-8')))