from __future__ import annotations
import asyncio
from dataclasses import dataclass
from crawl4ai import BrowserConfig
from typing import Any
from source.broker import InterProcessGateway, JsonMessageModel, IPGConsumerConfig
from source.database import HandshakeRawJobListingsRepo
from source.crawlers.base import CrawlerFactory, CrawlerFactoryConfig
from source.crawlers.handshake.hooks.handshake_p1_listings_hooks import create_handshake_p1_after_goto_hook
from source.crawlers.handshake.handshake_auth import HandshakeAuth


@dataclass
class HandshakeExtractListingsConfig:
    source_topics = ['extract.handshake.job.stage1.v1']
    msg_model = JsonMessageModel()
    auth: HandshakeAuth = HandshakeAuth()
    base_url: str = "https://app.joinhandshake.com/job-search/?page={}&per_page={}"

    def get_crawler(self) -> CrawlerFactoryConfig:
        return CrawlerFactory(
            CrawlerFactoryConfig(
                browser_config=BrowserConfig(
                    headless=False
                ),
                hooks={
                    'after_goto': create_handshake_p1_after_goto_hook()
                }
            )
        ).create_crawler()
    

class HandshakeExtractListings:

    def __init__(
        self, 
        config: HandshakeExtractListingsConfig,
        broker: InterProcessGateway, 
        repo: HandshakeRawJobListingsRepo
    ):
        self.config = config
        self.broker = broker
        self.repo = repo
        self.crawler = config.get_crawler()

    @property
    def consumer_info(self) -> IPGConsumerConfig:
        return IPGConsumerConfig(
            topics=self.config.source_topics,
            model=self.config.msg_model,
            notify=self.on_notify
        )

    def on_notify(self, payload: dict[str, Any]) -> None:
        match payload.get('action', None):
            case 'START_EXTRACTION':
                if isinstance((params := payload.get('params', None)), dict)\
                and isinstance((start_page := params.get('start_page', None)), int) \
                and isinstance((end_page := params.get('end_page', None)), int) \
                and isinstance((per_page := params.get('per_page', None)), int) \
                and 1 <= start_page <= end_page \
                and 1 <= per_page <= 50:
                    asyncio.run(self.extract(start_page, end_page, per_page))
            case _:
                pass
        return

    async def extract(self, start_page: int, end_page: int, per_page: int):
        urls = [
            self.config.base_url.format(page, per_page) 
            for page in range(start_page, end_page + 1)
        ]
        _ = urls
        await self.crawler.start()
        await self.crawler.close()
