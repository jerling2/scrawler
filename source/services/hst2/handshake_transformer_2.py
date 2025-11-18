import asyncio
from datetime import datetime
from dataclasses import dataclass
from source.utilities import Stock
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
        broker: InterProcessGateway,
        repo: HandshakeRepoT2,
        config: HandshakeTransformer2Config = HandshakeTransformer2Config()
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

    async def transform(self, url: str, html: str, scraped_at: datetime):
        raw_container = HandshakeRawDataContainer(html)
        clean_container = HandshakeCleanDataContainer(raw_container, scraped_at)
        clean_data = {
            **clean_container.get_all(),
            'scraped_at': scraped_at,
            'url': url
        }
        stock = Stock(clean_data)
        message_props = stock.collect(HandshakeLoader1Codec.Props)
        message = HandshakeLoader1Codec(**message_props)
        self.repo.insert(clean_data)
        self.broker.send(HandshakeLoader1Codec, HandshakeLoader1Codec.TOPIC, message)