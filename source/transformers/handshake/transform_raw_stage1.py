import asyncio
from dataclasses import dataclass
from crawl4ai import AsyncWebCrawler, BrowserConfig
from source.broker import InterProcessGateway, IPGConsumer
from source.codec import HandshakeTransformer1Codec
from source.crawlers import CrawlerFactory, CrawlerFactoryConfig
from source.database import StagedHandshakeJobStage1Repo


@dataclass
class TransformRawHandshakeJobStage1Config:
    source_topics = ['raw.handshake.job.stage1.v1']
    codec = HandshakeTransformer1Codec

    def get_crawler(self) -> AsyncWebCrawler:
        return CrawlerFactory(
            CrawlerFactoryConfig(
                browser_config=BrowserConfig(
                    headless=True,
                ),
                hooks={}
            )
        ).create_crawler()


class TransformRawHandshakeJobStage1:

    def __init__(
        self, 
        config: TransformRawHandshakeJobStage1Config,
        broker: InterProcessGateway,
        repo: StagedHandshakeJobStage1Repo
    ) -> None:
        self.config = config
        self.broker = broker
        self.repo = repo
        self.crawler = config.get_crawler()

    @property
    def consumer_info(self) -> IPGConsumer:
        return IPGConsumer(
            topics=self.config.source_topics,
            codec=self.config.codec,
            notify=self.on_notify
        )

    def on_notify(self, message: HandshakeTransformer1Codec):
        match message.action:
            case 'START_TRANSFORM':
                asyncio.run(self.transform(message.html))
            case _:
                pass
        return

    async def transform(self, html: str):
        pass