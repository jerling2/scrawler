from __future__ import annotations
import base64
import json
import zlib
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeTransformer1Codec:
    TOPIC = 'raw.handshake.job.stage1.v1'

    html: str
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
                'b64': base64.b64encode(self.compressed_html).decode('utf-8')
            }
        }

    @classmethod
    def serialize(cls, message: HandshakeTransformer1Codec) -> bytes:
        return json.dumps(message.payload).encode('utf-8')

    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeTransformer1Codec:
        payload = json.loads(message.decode('utf-8'))
        action = payload['action']
        b64_html = payload['params']['b64']
        enc_html = base64.b64decode(b64_html)
        html = zlib.decompress(enc_html)
        return cls(html=html, action=action)
