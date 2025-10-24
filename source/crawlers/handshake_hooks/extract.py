import re
from source.crawlers.base.types import AfterGotoProtocol
from playwright.async_api import Page, BrowserContext, Response, TimeoutError as PlaywrightTimeoutError
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


def handshake_extractor_2_hook() -> AfterGotoProtocol:

    async def after_goto(page: Page, context: BrowserContext, url: str, response: Response, **kwargs):
        _ = context, url, response, kwargs

        async def reload_page():
            await page.reload()

        async def is_page_loaded() -> bool:
            try:
                await page.get_by_role("button", name="Menu button").first.wait_for(state='visible', timeout=15_000)
                return True
            except Exception as e:
                return False
            
        async def is_redirect() -> bool:
            try:
                await page.get_by_role("button", name="Open profile options").wait_for(state="visible", timeout=5000)
                return True
            except Exception as e:
                return False

        async def if_exists_click_on_more_button() -> None:
            try:
                await page.get_by_role("button", name="More").click(timeout=5_000)
            except PlaywrightTimeoutError:
                return #< This is OK because there's not always 'More' button.
            except Exception as e:
                raise e

        @async_exponential_backoff_with_jitter(max_retries=5, base_delay=2, max_delay=30, callback_on_retry=reload_page)
        async def load_page() -> bool:
            if await is_page_loaded():
                await if_exists_click_on_more_button()
                return True
            elif await is_redirect():
                return False
            else:
                raise TimeoutError('Connection is throttled... backing off')
        
        if not await load_page():
            raise ValueError("Unable to fetch page: maximum retry limit reached, access denied, or page not found.")
            
        return page

    return after_goto