import re
import json
import asyncio
from dataclasses import dataclass
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, JsonCssExtractionStrategy, CacheMode
from source.broker import InterProcessGateway, IPGConsumer
from source.codec import HandshakeTransformer1Codec, HandshakeExtractor2Codec
from source.crawlers import CrawlerFactory, CrawlerFactoryConfig
from source.database import HandshakeRepoT1


@dataclass
class HandshakeTransformer1Config:
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


class HandshakeTransformer1:

    def __init__(
        self, 
        config: HandshakeTransformer1Config,
        broker: InterProcessGateway,
        repo: HandshakeRepoT1
    ) -> None:
        self.config = config
        self.broker = broker
        self.repo = repo
        self.crawler = config.get_crawler()

    @property
    def extraction_strategy(self) -> JsonCssExtractionStrategy:
        return JsonCssExtractionStrategy({
            'baseSelector': 'main',
            'fields': [
                {
                    'name': 'jobs',
                    'selector': 'a[role="button"]',
                    'type': 'list',
                    'fields': [
                        {
                            'name': 'url',
                            'type': 'attribute',
                            'attribute': 'href'
                        },
                        {
                            'name': 'role',
                            'type': 'attribute',
                            'attribute': 'aria-label'
                        },
                    ]
                },
            ]
        })

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

    def get_id(self, url: str) -> int:
        pattern = r'(?<=job-search/)\d+'
        if (match := re.search(pattern, url)):
            return int(match.group())
        raise ValueError

    def clean_role(self, raw_role: str) -> str:
        pattern = r'(?<=View\s).*'
        if (match := re.search(pattern, raw_role)):
            return match.group()
        raise ValueError

    def process(self, extracted_content: str) -> list[HandshakeExtractor2Codec]:
        messages = []
        content = json.loads(extracted_content)
        items = content[0]['jobs']
        for item in items:
            job_id = self.get_id(item['url'])
            role = self.clean_role(item['role'])
            url = f'https://app.joinhandshake.com/jobs/{job_id}'
            messages.append(HandshakeExtractor2Codec(job_id, role, url))
        return messages

    async def transform(self, html: str):
        config = CrawlerRunConfig(
            extraction_strategy=self.extraction_strategy,
            cache_mode=CacheMode.BYPASS
        )
        await self.crawler.start()
        result = await self.crawler.arun(f'raw:{html}', config)
        await self.crawler.close()
        if not result.success:
            return
        messages = self.process(result.extracted_content)
        self.repo.insert_many([
            (msg.job_id, msg.role, msg.url) 
            for msg in messages
        ])
        for msg in messages:
            self.broker.send(HandshakeExtractor2Codec, HandshakeExtractor2Codec.TOPIC, msg)
