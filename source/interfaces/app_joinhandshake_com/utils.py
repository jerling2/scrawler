from playwright.async_api import (
    Page,
    Locator,
    BrowserContext,
    expect
)


async def wait_to_be_visible_or_retry(page: Page, locator: Locator, timeout=30_000, max_retries=5):
    attempts = 0
    while attempts < max_retries:
        try:
            await expect(locator).to_be_visible(timeout=timeout)
            return
        except:
            attempts += 1
        if attempts < max_retries:
            print(f'\x1b[93m[RELOAD].. {attempts} {page.url}\x1b[0m')
            await page.reload()
    print(f'\x1b[31m[MAX RELOADS] {attempts} {page.url}\x1b[0m')
    return 