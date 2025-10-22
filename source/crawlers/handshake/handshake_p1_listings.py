from __future__ import annotations
import os
from dataclasses import dataclass
from crawl4ai import BrowserConfig
from typing import Callable, Any
from source.crawlers.base import CrawlerFactory, CrawlerFactoryConfig
from source.crawlers.handshake.hooks.handshake_p1_listings_hooks import create_handshake_p1_after_goto_hook
from source.crawlers.handshake.handshake_auth import HandshakeAuth
from source.broker import InterProcessGateway, IPGConsumerConfig, JsonMessageModel, MessageInterface
from source.database.data_lake import HandshakeRawJobListingsRepo


@dataclass
class HandshakeExtractListingsConfig:
    page_start: int
    page_end: int
    per_page: int
    auth: HandshakeAuth = HandshakeAuth()
    base_url: str = "https://app.joinhandshake.com/job-search/?page={}&per_page={}"

    def __post_init__(self):
        if not (1 <= self.page_start):
            raise ValueError('page_start must be greater than 0')
        if not (self.page_start <= self.page_end):
            raise ValueError('page_start must be smaller than or equal to the page_end')
        if not (1 <= self.per_page <= 50):
            raise ValueError(f'per_page must be within the interval: [1, 50]')

    @classmethod 
    def from_env(cls) -> HandshakeExtractListingsConfig:
        page_start = int(os.environ['HANDSHAKE_LISTINGS_PAGE_START'])
        page_end = int(os.environ['HANDSHAKE_LISTINGS_PAGE_END'])
        per_page = int(os.environ['HANDSHAKE_LISTINGS_PER_PAGE'])
        return cls(page_start, page_end, per_page)
    
    def get_handshake_listings_urls(self) -> list[str]:
        return \
        [
            self.base_url.format(page, self.per_page)
            for page in range(self.page_start, self.page_end + 1)
        ]
        
    def get_crawler_config(self) -> CrawlerFactoryConfig:
        return CrawlerFactoryConfig(
            browser_config=BrowserConfig(headless=False),
            hooks={
                'after_goto': create_handshake_p1_after_goto_hook()
            }
        )
    

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
        self.crawler = CrawlerFactory(
            config=self.config.get_crawler_config()
        ).create_crawler()
        
    def get_ipg_consumer_config(self):
        return IPGConsumerConfig(
            topics=['handshake.command.extract-jobs-stage1'],
            model=JsonMessageModel(),
            notify=self.on_notify
        )

    def on_notify(self, payload: dict[str, Any]) -> None:
        action = payload.get('action', None)
        if action == 'START_EXTRACTION':
            self.extract()
            return
        
    def extract(self):
        pass 