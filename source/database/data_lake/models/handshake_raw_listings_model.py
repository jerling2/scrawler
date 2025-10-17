from datetime import datetime
from typing import Literal
from dataclasses import dataclass


@dataclass(frozen=True)
class HandshakeRawListingsEntity:
    url: str
    html: str


@dataclass(frozen=True)
class HandshakeRawListingsModel:
    source: str
    url: str
    created_at: datetime
    compression_alg: Literal["zlib"]
    html_payload: bytes