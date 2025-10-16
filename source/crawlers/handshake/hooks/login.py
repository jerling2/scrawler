from playwright.async_api import Page, BrowserContext


def create_login_hook(login_url: str, username: str, password: str, session_storage: str):

    async def on_page_context_created(page: Page, context: BrowserContext, **kwargs):
        _ = kwargs

        await page.goto(login_url)
        await page.get_by_role('textbox', name='email address').fill(username)
        await page.get_by_text("Sign up Log in No account?").click() # remove focus from email input so the Next button becomes enabled
        await page.get_by_role('button', name='Next').click()
        await page.get_by_role('link', name='Log in another way').click()
        await page.get_by_role('textbox', name='Password').fill(password)
        await page.get_by_role('button', name='submit').click()
        await page.get_by_role('button', name='Open profile options').wait_for(state="visible")
        await context.storage_state(path=session_storage)
        return page

    return on_page_context_created