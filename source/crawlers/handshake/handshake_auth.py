import os
from dataclasses import dataclass
from pathlib import Path
from crawl4ai import BrowserConfig
from source.crawlers.base import CrawlerFactory, CrawlerFactoryConfig
from source.crawlers.handshake.hooks import create_login_hook, create_check_auth_hook


def make_login_crawler_config(login_url: str, username: str, password: str, session_storage: str):
    return CrawlerFactoryConfig(
        browser_config=BrowserConfig(
            headless=False
        ),
        hooks={
            'on_page_context_created': create_login_hook(login_url, username, password, session_storage)
        }
    )


def make_auth_check_crawler_config(auth_url: str, session_storage: str):
    if not (session_storage_path := Path(session_storage)).exists():
        session_storage_path.write_text("{}")
    return CrawlerFactoryConfig(
        browser_config=BrowserConfig(
            headless=False,
            storage_state=session_storage
        ),
        hooks={
            'on_page_context_created': create_check_auth_hook(auth_url)
        }
    )


@dataclass(frozen=True)
class HandshakeAuthConfig:
    username: str
    password: str
    session_storage: str
    login_url: str = "https://app.joinhandshake.com/login"
    auth_url: str = "https://app.joinhandshake.com/explore"

    def get_login_crawler_config(self):
        return make_login_crawler_config(self.login_url, self.username, self.password, self.session_storage)

    def get_auth_check_crawler_config(self):
        return make_auth_check_crawler_config(self.auth_url, self.session_storage)

    @classmethod
    def from_env(cls):
        username = os.environ['USER_APP_HANDSHAKE_COM']
        password = os.environ['PASS_APP_HANDSHAKE_COM']
        session_storage = str(Path(os.environ['SESSION_STORAGE']) / 'handshake.json')
        return cls(username, password, session_storage)


class HandshakeAuth:

    def __init__(self, config = HandshakeAuthConfig.from_env()) -> None:
        self.config = config
        self.login_crawler = CrawlerFactory(self.config.get_login_crawler_config()).create_crawler()
        self.auth_check_crawler = CrawlerFactory(self.config.get_auth_check_crawler_config()).create_crawler()

    async def login(self):
        await self.login_crawler.start()
        result = await self.login_crawler.arun(url=self.config.auth_url)
        await self.login_crawler.close()

    async def check_auth(self) -> bool:
        await self.auth_check_crawler.start()
        result = await self.auth_check_crawler.arun(url=self.config.auth_url)
        await self.auth_check_crawler.close()
        return result.success