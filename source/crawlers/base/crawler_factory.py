from dataclasses import dataclass
from crawl4ai import BrowserConfig, AsyncWebCrawler
from source.crawlers.base.types import HookHandler


@dataclass
class CrawlerFactoryConfig:
    browser_config: BrowserConfig
    hooks: HookHandler


class CrawlerFactory:

    def __init__(self, config: CrawlerFactoryConfig) -> None:
        self.config = config

    def create_crawler(self) -> AsyncWebCrawler:
        crawler = AsyncWebCrawler(config=self.config.browser_config)
        for hook_name, hook_func in self.config.hooks.items():
            crawler.crawler_strategy.set_hook(hook_name, hook_func)
        return crawler