import re
from source.crawlers.base.types import AfterGotoProtocol
from playwright.async_api import Page, BrowserContext, Response
from source.utilities import async_exponential_backoff_with_jitter


def handshake_extractor_1_hook() -> AfterGotoProtocol:

    async def after_goto(page: Page, context: BrowserContext, url: str, response: Response, **kwargs):
        _ = context, url, response, kwargs

        async def reload_page():
            await page.reload()

        @async_exponential_backoff_with_jitter(max_retries=5, base_delay=2, max_delay=30, callback_on_retry=reload_page)
        async def wait_for_page():
            await page.get_by_role("button", name=re.compile(r"^view", re.IGNORECASE)).first.wait_for(state="visible", timeout=15_000)
        
        try:
            await wait_for_page()
        except:
            await page.pause()
            
        return page

    return after_goto
