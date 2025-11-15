import re
import json
import asyncio
from datetime import datetime
from dataclasses import dataclass
from source.broker import InterProcessGateway, IPGConsumer
from source.codec import HandshakeTransformer2Codec, HandshakeLoader1Codec
from source.database import HandshakeRepoT2
from source.services.hst2.raw import HandshakeRawDataContainer
from source.services.hst2.clean import HandshakeCleanDataContainer


@dataclass
class HandshakeTransformer2Config:
    source_topics = ['raw.handshake.job.stage2.v1']
    codec = HandshakeTransformer2Codec


class HandshakeTransformer2:

    def __init__(
        self, 
        config: HandshakeTransformer2Config,
        broker: InterProcessGateway,
        repo: HandshakeRepoT2
    ) -> None:
        self.config = config
        self.broker = broker
        self.repo = repo

    @property
    def consumer_info(self) -> IPGConsumer:
        return IPGConsumer(
            topics=self.config.source_topics,
            codec=self.config.codec,
            notify=self.on_notify
        )

    def on_notify(self, message: HandshakeTransformer2Codec):
        match message.action:
            case 'START_TRANSFORM':
                asyncio.run(self.transform(message.url, message.html, message.created_at))
            case _:
                pass
        return

    async def transform(self, url: str, html: str, created_at: datetime):
        clean = HandshakeCleanDataContainer(
            HandshakeRawDataContainer(html),
            created_at
        ).get_all()
        message = HandshakeLoader1Codec(
            url=url,
            overview=clean['about'],
            posted_at=clean['posted_at'],
            apply_by=clean['apply_by'],
            documents=clean['documents'],
            company=clean['company'],
            industry=clean['industry'],
            role=clean['position'],
            apply_type=clean['apply_type'],
            wage=clean['wage'],
            location_type=clean['location_type'],
            location=clean['location'],
            job_type=clean['job_type'],
            employment_type=clean['employment_type']
        )
        self.broker.send(HandshakeLoader1Codec, HandshakeLoader1Codec.TOPIC, message)