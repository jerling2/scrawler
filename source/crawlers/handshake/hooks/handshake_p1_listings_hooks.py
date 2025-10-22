from source.crawlers.base.types import AfterGotoProtocol
from playwright.async_api import Page, BrowserContext, Response


def create_handshake_p1_after_goto_hook() -> AfterGotoProtocol:

    async def after_goto(page: Page, context: BrowserContext, url: str, response: Response, **kwargs):
        _ = context, url, response, kwargs
        await page.pause()
        return page

    return after_goto
