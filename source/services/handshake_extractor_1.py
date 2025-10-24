import os
import asyncio
from dataclasses import dataclass
from pathlib import Path
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode, AsyncWebCrawler, MemoryAdaptiveDispatcher, RateLimiter
from source.broker import InterProcessGateway, IPGConsumer
from source.database import HandshakeRepoE1
from source.codec import HandshakeExtractor1Codec, HandshakeTransformer1Codec
from source.crawlers.base import CrawlerFactory, CrawlerFactoryConfig
from source.crawlers import handshake_extractor_1_hook
from source.services.handshake_auth import HandshakeAuth


@dataclass
class HandshakeExtractor1Config:
    topics = ['extract.handshake.job.stage1.v1']
    codec = HandshakeExtractor1Codec
    base_url: str = "https://app.joinhandshake.com/job-search/?page={}&per_page={}"

    def get_auth(self) -> HandshakeAuth:
        return HandshakeAuth()

    def get_crawler(self) -> AsyncWebCrawler:
        return CrawlerFactory(
            CrawlerFactoryConfig(
                browser_config=BrowserConfig(
                    headless=True,
                    storage_state=Path(os.environ['SESSION_STORAGE']) / 'handshake.json'
                ),
                hooks={
                    'after_goto': handshake_extractor_1_hook()
                }
            )
        ).create_crawler()
    

class HandshakeExtractor1:

    def __init__(
        self, 
        config: HandshakeExtractor1Config,
        broker: InterProcessGateway, 
        repo: HandshakeRepoE1
    ):
        self.config = config
        self.broker = broker
        self.repo = repo
        self.auth = config.get_auth()
        self.crawler = config.get_crawler()

    @property
    def consumer_info(self) -> IPGConsumer:
        return IPGConsumer(
            topics=self.config.topics,
            codec=self.config.codec,
            notify=self.on_notify
        )

    def on_notify(self, message: HandshakeExtractor1Codec) -> None:
        match message.action:
            case 'START_EXTRACT':
                asyncio.run(self.extract(
                    message.start_page, message.end_page, message.per_page
                ))
            case _:
                pass
        return

    async def extract(self, start_page: int, end_page: int, per_page: int):
        urls = [
            self.config.base_url.format(page, per_page) 
            for page in range(start_page, end_page + 1)
        ]
        run_config = CrawlerRunConfig(stream=True, cache_mode=CacheMode.BYPASS)
        dispatcher = MemoryAdaptiveDispatcher(
            max_session_permit=5,
            rate_limiter=RateLimiter()
        )
        await self.auth.login()
        async for result in await self.crawler.arun_many(urls, run_config, dispatcher):
            if result.success:
                self.push_to_repo(result.url, result.html)
                self.propogate_message(result.html)

    def push_to_repo(self, url: str, html: str):
        self.repo.insert(url, html)
    
    def propogate_message(self, html: str):
        message = HandshakeTransformer1Codec(html)
        self.broker.send(HandshakeTransformer1Codec, HandshakeTransformer1Codec.TOPIC, message)