from playwright.async_api import Page, BrowserContext


def handshake_check_auth_hook(auth_url):

    async def on_page_context_created(page: Page, context: BrowserContext, **kwargs):
        _ = context, kwargs
        
        await page.goto(auth_url)
        if auth_url != page.url:
            raise AssertionError(f"Expected page {auth_url} but got {page.url}")
        await page.get_by_role("button", name="Open profile options").wait_for(state="visible", timeout=5000)
        return page

    return on_page_context_created