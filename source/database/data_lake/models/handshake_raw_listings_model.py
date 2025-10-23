from typing import Any
from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeRawJobListingsEntity:
    url: str
    html: str


@dataclass(frozen=True)
class HandshakeRawJobListingsModel:
    source: str = 'handshake'
    codec: str = 'zlib'

    def make_document(
            self,
            created_at: datetime, 
            url: str, 
            html_payload: bytes
        ) -> dict[str, Any]:
        return \
        {
            'source': self.source,
            'created_at': created_at,
            'url': url,
            'compression_alg': self.codec,
            'html_payload': html_payload
        }
