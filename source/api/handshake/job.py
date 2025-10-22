import base64
from dataclasses import dataclass
from typing import Any
from source.broker import JsonMessageModel
from source.utilities import zlib_compress


@dataclass(frozen=True)
class APIExtractHandshakeJobStage1:
    start_page: int = 1
    end_page: int = 40
    per_page: int = 50
    source_topic = 'extract.handshake.job.stage1.v1'
    model = JsonMessageModel()

    @property 
    def payload(self) -> dict[str, Any]:
        return \
        {
            'action': 'START_EXTRACTION',
            'params': {
                'start_page': self.start_page,
                'end_page': self.end_page,
                'per_page': self.per_page
            }
        }
    

@dataclass(frozen=True)
class APIRawHandshakeJobStage1:
    raw_html: str
    source_topic = 'raw.handshake.job.stage1.v1'
    model = JsonMessageModel()

    @property
    def payload(self) -> dict[str, Any]:
        return \
        {
            'codec': 'zlib',
            'binary': base64.b64encode(zlib_compress(self.raw_html)).decode('utf-8')
        }