import os
import threading
import asyncio
import json
import queue
from dataclasses import dataclass
from pathlib import Path
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, MemoryAdaptiveDispatcher, RateLimiter, CacheMode, JsonCssExtractionStrategy
from source.crawlers import CrawlerFactory, CrawlerFactoryConfig, handshake_extractor_2_hook
from source.services.handshake_auth import HandshakeAuth, HandshakeAuthConfig
from source.codec import HandshakeExtractor2Codec, HandshakeTransformer2Codec
from source.broker import InterProcessGateway, IPGConsumer
from source.database import HandshakeLake


@dataclass(frozen=True)
class HandshakeExtractor2Config:
    TOPICS = ['extract.handshake.job.stage2.v1']
    CODEC = HandshakeExtractor2Codec
    SESSION_NAME = 'handshake_e2'
    MSG_BUF_SIZE = 100
    MSG_BUF_TIMEOUT = 30

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

    def __init__(self,
            broker: InterProcessGateway,
            repo: HandshakeLake,
            config: HandshakeExtractor2Config = HandshakeExtractor2Config()
        ):
        self.config = config
        self.crawler = config.get_crawler()
        self.auth = config.get_auth()
        self.broker = broker
        self.repo = repo
        self.msg_buf = queue.Queue()  #< Thread-safe
        self.buf_size = config.MSG_BUF_SIZE
        self.buf_timeout = config.MSG_BUF_TIMEOUT
        self._buf_event = threading.Event()
        self._worker = threading.Thread(target=self._worker, daemon=True)
        self._worker.start()
        self._worker_stop_event = threading.Event()
        self._worker_closed_event = threading.Event()

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
        if self.msg_buf.qsize() >= self.buf_size:
            self._buf_event.set()

    def _worker(self):
        while True:
            self._buf_event.wait(self.buf_timeout)
            if not self.msg_buf and self._worker_stop_event.is_set():
                self._buf_event.clear()
                break
            if self.msg_buf.qsize() == 0:
                # Using qsize opens a risk of race condition. Though, a race condition won't cause 
                # damage because the system will process the queue in time (regardless).
                self._buf_event.clear()
                continue
            batch = []
            while True:
                try:
                    batch.append(self.msg_buf.get_nowait())
                except queue.Empty:
                    break
            urls = [msg.url for msg in batch]
            asyncio.run(self.extract(urls))
            self._buf_event.clear()
            if self._worker_stop_event.is_set():
                break
        self._worker_closed_event.set()

    def shutdown(self):
        self._worker_stop_event.set()
        self._worker_closed_event.wait()

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
                self.propogate_message(result.url, html)
                self.repo.set_e2_success(result.url, True)
            else:
                self.repo.set_e2_success(result.url, False)
        await self.crawler.close()
        
    def propogate_message(self, url: str, html: str):
        message = HandshakeTransformer2Codec(url, html)
        self.broker.send(HandshakeTransformer2Codec, HandshakeTransformer2Codec.TOPIC, message)