from __future__ import annotations
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeExtractor1Codec:
    TOPIC = 'extract.handshake.job.stage1.v1'

    start_page: int = 1
    end_page: int = 40
    per_page: int = 50
    action: str = 'START_EXTRACT'

    @property
    def payload(self):
        return {
            'action': self.action,
            'params': {
                'start_page': self.start_page,
                'end_page': self.end_page,
                'per_page': self.per_page
            }
        }

    @classmethod
    def serialize(cls, message: HandshakeExtractor1Codec) -> bytes:
        return json.dumps(message.payload).encode()
        
    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeExtractor1Codec:
        payload = json.loads(message.decode())
        return cls(action=payload['action'], **payload['params'])
