from typing import Any, Protocol, TypedDict
from playwright.async_api import Page, BrowserContext, Response


class AfterGotoProtocol(Protocol):
    async def __call__(
        self,
        page: Page,
        context: BrowserContext,
        url: str,
        response: Response,
        **kwargs: Any
    ) -> Any: ...


class OnPageContextProtocol(Protocol):
    async def __call__(
        self,
        page: Page,
        context: BrowserContext,
        **kwargs: Any
    ) -> Any: ...


class HookHandler(TypedDict, total=False):
    on_page_context_created: OnPageContextProtocol
    after_goto: AfterGotoProtocol