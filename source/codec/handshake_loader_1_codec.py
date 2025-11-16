from __future__ import annotations
import base64
import json
import zlib
from datetime import datetime
from dataclasses import dataclass, fields
from typing import TypedDict, Literal
from pydantic import TypeAdapter, ValidationError
from source.utilities import classproperty, as_typed_dict


@dataclass(frozen=True)
class HandshakeLoader1Codec:
    TOPIC = 'load.handshake.job.v1'
    ACTION = 'START_LOAD'
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

    @classproperty(cached=True)
    def Props(cls) -> type[dict]:
        return as_typed_dict(cls)

    @classproperty(cached=True)
    def SerializableProps(cls) -> type[dict]:
        serializable_props = TypedDict('SerializedProps', {
            **cls.Props.__annotations__.copy(),
            'topic': Literal['load.handshake.job.v1'],
            'action': Literal['START_LOAD'],
            'about_codec': Literal['zlib'],
            'apply_by': str | None,
            'posted_at': str | None
        })
        return serializable_props
    
    @classproperty(cached=True)
    def serial_props_ta(cls):
        return TypeAdapter(cls.SerializableProps)
      
    @property
    def compressed_about(self) -> str:
        about_bytes = self.about.encode('utf-8')
        about_compressed_bytes = zlib.compress(about_bytes)
        about_compressed_b64 = base64.b64encode(about_compressed_bytes)
        about_compressed_str = about_compressed_b64.decode('utf-8')
        return about_compressed_str

    @property
    def payload(self) -> HandshakeLoader1Codec.SerializableProps:
        EXCLUDE_KEYS = ['about', 'apply_by', 'posted_at']
        serializable_dict = {
            **{field.name: getattr(self, field.name) for field in fields(self) if field.name not in EXCLUDE_KEYS},
            'topic': self.TOPIC,
            'action': self.ACTION,
            'about_codec': 'zlib',
            'about': self.compressed_about,
            'apply_by': str(self.apply_by),
            'posted_at': str(self.posted_at)
        }
        try:
            serializable_dict = self.serial_props_ta.validate_python(serializable_dict)
        except ValidationError:
            print('Unexpected Error: payload could not be coerced to the correct type')
            raise
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
            serial_props = cls.serial_props_ta.validate_python(payload)
        except ValidationError:
            print('Error: the serialized message cannot be coerced to the correct type')
            raise
        props: HandshakeLoader1Codec.Props = {
            **{k: v for k, v in serial_props.items() if k not in EXCLUDE_SERIAL_KEYS},
            'about': cls._decompress_about(serial_props['about']),
            'apply_by': cls._to_datetime(serial_props['apply_by']),
            'posted_at': cls._to_datetime(serial_props['posted_at'])
        }
        return cls(props)
