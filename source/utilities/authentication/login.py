import os
import re
from pathlib import Path
from crawl4ai import (
    BrowserConfig,
    AsyncWebCrawler
)
from playwright.async_api import (
    Page,
    BrowserContext,
    expect
)


class LoginProcedures:

    def __init__(self):
        session_storage = os.getenv("SESSION_STORAGE")
        if not session_storage:
            raise Exception(f"LoginProcedures: requires session_storage")
        self.session_storage = Path(session_storage)
        if not self.session_storage.exists():
            raise Exception(f'Error: {path!r} does not exist')
        self.procedures = {
            'https://uoregon.joinhandshake.com': self.uoregon_handshake_auth
        }

    def __contains__(self, url):
        return url in self.procedures

    def uoregon_handshake_auth(self, browser_config) -> AsyncWebCrawler:
        storage_state_path = self.session_storage / "handshake.json"
        if storage_state_path.exists():
            browser_config = browser_config.clone(storage_state=str(storage_state_path)) 
        username = os.getenv("USER_APP_HANDSHAKE_COM")
        password = os.getenv("PASS_APP_HANDSHAKE_COM")
        crawler = AsyncWebCrawler(config=browser_config)
        async def on_page_context_created(page: Page, context: BrowserContext, **kwargs):
            print("\x1b[36m[AUTH].... \u2192 Checking credentials\x1b[0m")
            await page.goto('https://uoregon.joinhandshake.com/login')
            if page.url == 'https://uoregon.joinhandshake.com/explore':
                print(f"\x1b[1;93m[SUCCESS]\x1b[0m")
                return page
            await page.locator(".sso-button").click()
            await page.get_by_placeholder("Username").fill(username)
            await page.get_by_placeholder("Password").fill(password)
            await page.get_by_role("button", name="Login").click()
            await expect(page.get_by_role("heading", name="Enter code in Duo Mobile")).to_be_visible(timeout=60_000)
            sso_code = await page.get_by_text(re.compile(r"^\d+$")).text_content()
            print(f"\x1b[1;93m[AUTH] SSO : {sso_code}\x1b[0m")
            await expect(page.get_by_role("button", name="Yes, this is my device")).to_be_visible(timeout=60_000)
            print(f"\x1b[1;93m[SUCCESS]\x1b[0m")
            await page.get_by_role("button", name="Yes, this is my device").click()
            await expect(page.get_by_role("heading", name="University of Oregon")).to_be_visible(timeout=60_000)
            await context.storage_state(path=storage_state_path)
            return page
        crawler.crawler_strategy.set_hook('on_page_context_created', on_page_context_created)
        return crawler

    def auth(self, url, browser_config):
        if url not in self.procedures:
            raise TypeError(f"{url} is not a recognized Login procedure.")
        return self.procedures[url](browser_config)