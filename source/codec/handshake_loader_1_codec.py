from __future__ import annotations
import base64
import json
import zlib
from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeLoader1Codec:
    TOPIC = 'load.handshake.job.v1'

    url: str
    overview: str
    posted_at: datetime
    apply_by: datetime
    documents: list[str]
    company: str
    industry: str
    role: str
    is_internal_apply: str
    wage: list[int] | None
    location_type: list[str]
    location: str
    job_type: str
    is_internship: bool
    action: str = 'START_LOAD'

    @property
    def compressed_overview(self) -> bytes:
        return zlib.compress(self.overview.encode('utf-8'))

    @property
    def payload(self):
        return {
            'enc': {
                'codec': 'zlib',
                'b64_overview': base64.b64encode(self.compressed_overview).decode('utf-8'),
            },
            'dates': {
                'posted_at': str(self.posted_at),
                'apply_by': str(self.apply_by),   
            },
            'primitives': {
                'url': self.url,
                'documents': self.documents,
                'company': self.company,
                'industry': self.industry,
                'role': self.role,
                'is_internal_apply': self.is_internal_apply,
                'wage': self.wage,
                'location_type': self.location_type,
                'location': self.location,
                'job_type': self.job_type,
                'is_internship': self.is_internship,
                'action': self.action
            }
        }

    @classmethod
    def serialize(cls, message: HandshakeLoader1Codec) -> bytes:
        return json.dumps(message.payload).encode('utf-8')

    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeLoader1Codec:
        payload = json.loads(message.decode('utf-8'))
        b64_overview = payload['enc']['b64_overview']
        enc_overview = base64.b64decode(b64_overview)
        overview = zlib.decompress(enc_overview).decode('utf-8')
        posted_at = datetime.fromisoformat(payload['dates']['posted_at'])
        apply_by = datetime.fromisoformat(payload['dates']['apply_by'])
        return cls(overview=overview, posted_at=posted_at, apply_by=apply_by, **payload['primitives'])