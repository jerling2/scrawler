from dataclasses import dataclass
from typing import Any
from source.broker import JsonMessageModel


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
            'targetTopic': 'raw.handshake.jobs.stage1.v1',
            'params': {
                'start_page': self.start_page,
                'end_page': self.end_page,
                'per_page': self.per_page
            }
        }