from __future__ import annotations
import base64
import json
import zlib
from datetime import datetime
from dataclasses import dataclass
from typing import TypedDict, Literal, get_type_hints
from pydantic import TypeAdapter, ValidationError


@dataclass(frozen=True)
class HandshakeLoader1Codec:
    TOPIC = 'load.handshake.job.v1'
    ACTION = 'START_LOAD'

    class Props(TypedDict):
        about: str | None
        apply_by: datetime | None
        apply_type: str | None
        company: str | None
        documents: list[str]
        employment_type: str | None
        industry: str | None
        job_type: str | None
        location: str | None
        location_type: list[str]
        position: str | None
        posted_at: datetime | None
        url: str | None
        wage: list[int, int] | None
    
    class SerialProps(TypedDict):
        about_codec: Literal['zlib'] 
        about: str
        apply_by: str
        apply_type: str
        company: str
        documents: list[str]
        employment_type: str
        industry: str
        job_type: str
        location: str
        location_type: list[str]
        position: str
        posted_at: str
        url: str
        wage: list[int, int]

    TA_PROPS = TypeAdapter(Props)
    TA_SERIAL_PROPS = TypeAdapter(SerialProps)
    props: Props

    def __post_init__(self):
        try:
            self.TA_PROPS.validate_python(self.props)
        except ValidationError:
            raise

    @property
    def compressed_about(self) -> str:
        about_bytes = self.props['about'].encode('utf-8')
        about_compressed_bytes = zlib.compress(about_bytes)
        about_compressed_b64 = base64.b64encode(about_compressed_bytes)
        about_compressed_str = about_compressed_b64.decode('utf-8')
        return about_compressed_str

    @property
    def payload(self) -> dict:
        UPDATE_KEYS = ['about', 'apply_by', 'posted_at']
        prop_keys = get_type_hints(HandshakeLoader1Codec.Props).keys()
        serializable_dict = {
            **{k: v for k, v in self.props.items() if k not in UPDATE_KEYS and k in prop_keys},
            'about_codec': 'zlib',
            'about': self.compressed_about,
            'apply_by': str(self.props['apply_by']),
            'posted_at': str(self.props['posted_at'])
        }
        return serializable_dict

    @classmethod
    def serialize(cls, message: HandshakeLoader1Codec) -> bytes:
        return json.dumps(message.payload).encode('utf-8')

    @staticmethod
    def _decompress_about(serial_props: HandshakeLoader1Codec.SerialProps) -> str:
        about_compressed_str = serial_props['about']
        about_compressed_b64 = base64.b64decode(about_compressed_str)
        about_bytes = zlib.decompress(about_compressed_b64)
        about = about_bytes.decode('utf-8')
        return about
        
    @staticmethod
    def _to_datetime(datetime_iso_string: str) -> datetime:
        return datetime.fromisoformat(datetime_iso_string)

    @classmethod
    def deserialize(cls, message: bytes) -> HandshakeLoader1Codec:
        EXCLUDE_SERIAL_KEYS = ['about_codec', 'about', 'apply_by', 'posted_at']
        payload = json.loads(message.decode('utf-8'))
        try: 
            serial_props = cls.TA_SERIAL_PROPS.validate_python(payload)
        except ValidationError:
            raise
        props: HandshakeLoader1Codec.Props = {
            **{k: v for k, v in serial_props.items() if k not in EXCLUDE_SERIAL_KEYS},
            'about': cls._decompress_about(serial_props['about']),
            'apply_by': cls._to_datetime(serial_props['apply_by']),
            'posted_at': cls._to_datetime(serial_props['posted_at'])
        }
        return cls(props)