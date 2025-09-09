import traceback
from crawl4ai import BrowserConfig
from .login import LoginProcedures


class AuthAgent():
    
    def __init__(self, url, browser_config=None):
        self.login_procedures = LoginProcedures()
        self.crawler = None
        self.url = url
        self.browser_config = browser_config or BrowserConfig()

    async def __aenter__(self):
        """ Execute immediately after entering an `async wait` block """
        if self.url in self.login_procedures:
            self.crawler = self.login_procedures.auth(self.url, self.browser_config)
        else:
            raise Exception(f"The url {self.url!s} is not registered in LoginProcedure")
        print("\x1b[36m[INIT].... \u2192 AuthAgent\x1b[0m")
        await self.crawler.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """ Execute just before leaving an `async wait` block """
        if self.crawler:
            await self.crawler.close()
            self.crawler = None
        if exc_type:
            print("Exception occurred!")
            formatted_tb = ''.join(traceback.format_exception(exc_type, exc_val, exc_tb))
            print("Detailed traceback:")
            print(formatted_tb)
        print("\x1b[36m[CLOSE].... \u2192 AuthAgent\x1b[0m")
        return False

    async def login(self):
        await self.crawler.arun(self.url)
