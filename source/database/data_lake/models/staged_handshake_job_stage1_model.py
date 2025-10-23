from typing import Any
from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class StagedHandshakeJobStage1Model:
    source = 'handshake'
    codec = 'zlib'

    def make_document(
            self,
            created_at: datetime, 
            staged_payload: bytes
        ) -> dict[str, Any]:
        return \
        {
            'source': self.source,
            'created_at': created_at,
            'codec': self.codec,
            'payload': staged_payload
        }
