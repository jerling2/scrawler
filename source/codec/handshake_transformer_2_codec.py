from __future__ import annotations
import base64
import json
import zlib
from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeTransformer2Codec:
    TOPIC = 'raw.handshake.job.stage2.v1'

    url: str
    html: str
    created_at: datetime = datetime.now()
    action: str = 'START_TRANSFORM'

    @property
    def compressed_html(self) -> bytes:
        return zlib.compress(self.html.encode('utf-8'))

    @property
    def payload(self):
        return {
            'action': self.action,
            'params': {
                'codec': 'zlib',
                'created_at': str(self.created_at),
                'url': self.url,
                'b64': base64.b64encode(self.compressed_html).decode('utf-8')
            }
        }

    @classmethod
    def serialize(cls, message: HandshakeTransformer2Codec) -> bytes:
        return json.dumps(message.payload).encode('utf-8')

    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeTransformer2Codec:
        payload = json.loads(message.decode('utf-8'))
        action = payload['action']
        url = payload['params']['url']
        created_at = datetime.fromisoformat(payload['params']['created_at'])
        b64_html = payload['params']['b64']
        enc_html = base64.b64decode(b64_html)
        html = zlib.decompress(enc_html)
        return cls(html=html, url=url, action=action, created_at=created_at)
