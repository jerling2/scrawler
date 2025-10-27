import os
import asyncio
import json
from queue import Queue
from dataclasses import dataclass
from pathlib import Path
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, MemoryAdaptiveDispatcher, RateLimiter, CacheMode, JsonCssExtractionStrategy
from source.crawlers import CrawlerFactory, CrawlerFactoryConfig, handshake_extractor_2_hook
from source.services.handshake_auth import HandshakeAuth, HandshakeAuthConfig
from source.codec import HandshakeExtractor2Codec, HandshakeTransformer2Codec
from source.broker import InterProcessGateway, IPGConsumer
from source.database import HandshakeRepoE2


@dataclass(frozen=True)
class HandshakeExtractor2Config:
    TOPICS = ['extract.handshake.job.stage2.v1']
    CODEC = HandshakeExtractor2Codec
    SESSION_NAME = 'handshake_e2'
    MSG_BUF_SIZE = 100

    def get_auth(self) -> HandshakeAuth:
        return HandshakeAuth(HandshakeAuthConfig.from_env(session_name=self.SESSION_NAME))

    def get_crawler(self) -> AsyncWebCrawler:
        return CrawlerFactory(CrawlerFactoryConfig(
            BrowserConfig(
                headless=True,
                storage_state=Path(os.environ['SESSION_STORAGE']) / f'{self.SESSION_NAME}.json'
            ),
            hooks={
                'after_goto': handshake_extractor_2_hook()
            }
        )).create_crawler()


class HandshakeExtractor2:

    def __init__(self, config: HandshakeExtractor2Config, broker: InterProcessGateway, repo: HandshakeRepoE2):
        self.config = config
        self.crawler = config.get_crawler()
        self.auth = config.get_auth()
        self.broker = broker
        self.repo = repo
        self.msg_buf = Queue()

    @property
    def consumer_info(self) -> IPGConsumer:
        return IPGConsumer(
            topics=self.config.TOPICS,
            codec=self.config.CODEC,
            notify=self.on_notify
        )

    @property
    def extraction_strategy(self) -> JsonCssExtractionStrategy:
        return JsonCssExtractionStrategy({
            'baseSelector': 'body',
            'baseFields': [{
                'name': 'main_html',
                'type': 'html'
            }],
            'fields': []
        })

    def on_notify(self, message: HandshakeExtractor2Codec):
        self.msg_buf.put(message)
        if self.msg_buf.qsize() >= self.config.MSG_BUF_SIZE:
            self.flush()

    def flush(self):
        extract_bucket = []
        dead_letter_bucket = []
        while self.msg_buf.qsize():
            message = self.msg_buf.get()
            match message.action:
                case 'START_EXTRACT':
                    extract_bucket.append(message)
                case _:
                    dead_letter_bucket.append(message)
        if (num_dead_letters := len(dead_letter_bucket)) > 0:
            print(f'Warning: caught {num_dead_letters} dead lettters')
        if len(extract_bucket) > 0:
            urls = [msg.url for msg in extract_bucket]
            asyncio.run(self.extract(urls))
        
    async def extract(self, urls: list[str]):
        run_config = CrawlerRunConfig(
            stream=True,
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=self.extraction_strategy,
        )
        dispatcher = MemoryAdaptiveDispatcher(
            max_session_permit=5,
            rate_limiter=RateLimiter()
        )
        await self.auth.login()
        await self.crawler.start()
        async for result in await self.crawler.arun_many(urls, run_config, dispatcher):
            if result.success:
                html = json.loads(result.extracted_content)[0]['main_html']
                self.push_to_repo(result.url, html)
                self.propogate_message(result.url, html)
        await self.crawler.close()
        
    def push_to_repo(self, url: str, html: str):
        self.repo.insert(url, html)
    
    def propogate_message(self, url: str, html: str):
        message = HandshakeTransformer2Codec(url, html)
        self.broker.send(HandshakeTransformer2Codec, HandshakeTransformer2Codec.TOPIC, message)