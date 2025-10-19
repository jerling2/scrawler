import json
from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class JsonMessageModel:
    
    def serialize(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload).encode()

    def deserialize(self, data: bytes) -> dict[str, Any]:
        return json.loads(data.decode())