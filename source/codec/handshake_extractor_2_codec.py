from __future__ import annotations
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeExtractor2Codec:
    TOPIC = 'extract.handshake.job.stage2.v1'

    job_id: int
    role: str
    url: str

    action: str = 'START_EXTRACT'

    @property
    def payload(self):
        return {
            'action': self.action,
            'params': {
                'job_id': self.job_id,
                'role': self.role,
                'url': self.url
            }
        }

    @classmethod
    def serialize(cls, message: HandshakeExtractor2Codec) -> bytes:
        return json.dumps(message.payload).encode()
        
    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeExtractor2Codec:
        payload = json.loads(message.decode())
        return cls(action=payload['action'], **payload['params'])
