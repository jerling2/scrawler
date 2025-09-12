from dataclasses import dataclass
from typing import Any, Optional
from ..utilities.utils import wait_to_be_visible_or_retry
from source import AuthAgent, Writer
import json
import os
import re
from pathlib import Path
from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig,
    BrowserConfig, 
    JsonCssExtractionStrategy,
    CacheMode,
    DefaultMarkdownGenerator,
    BM25ContentFilter,
    MemoryAdaptiveDispatcher,
    RateLimiter
)
from playwright.async_api import (
    Page,
    Locator,
    BrowserContext,
    expect
)


SESSION = Path(os.getenv("SESSION_STORAGE")) / "handshake.json"

LOGIN_URL = "https://uoregon.joinhandshake.com"

RELEVANT_JOB_URL = "https://app.joinhandshake.com/job-search/?page={page}&per_page={per_page}"

RELEVANT_JOB_HTML_SCHEMA = \
"""
{
    "name": "Handshake Jobs: (ID, Title, URL) ",
    "baseSelector": "main",
    "fields": [
        {
            "name": "jobs",
            "selector": "a[role='button']",
            "type": "list",
            "fields": [
                {
                    "name": "job_id",
                    "type": "attribute",
                    "attribute": "href"
                },
                {
                    "name": "position",
                    "type": "attribute",
                    "attribute": "aria-label"
                },
                {
                    "name": "href",
                    "type": "attribute",
                    "attribute": "href"
                }
            ]
        }
    ] 
}
"""

async def after_goto(page: Page, context: BrowserContext, url: str, response, **kwargs):
    locator = page.get_by_role("button", name=re.compile(r"View.*"))
    url_pattern=r'^.*/job-search/.*$'
    await wait_to_be_visible_or_retry(page, locator.first, url_pattern=url_pattern)
    return page

def serialize(buffer: list[Any]) -> list[str]:
    serialized_buffer = []
    for item in buffer:
        jobs = item['jobs']
        for job in jobs:
            job_id = re.search(r'/job-search/(\d+)', job['job_id']).group(1)
            position = job['position'][5:]
            href = "https://app.joinhandshake.com/jobs/" + job_id
            serialized_buffer.append([job_id, position, href])
    return serialized_buffer


def deserialize(row: list[str]) -> list[dict[str, Any]]:
    return {
        'job_id': row[0],
        'position': row[1],
        'url': row[2]
    }


@dataclass
class JobSearchState:
    file: Path
    start: int
    end: int
    per_page: int
    save_frequency: Optional[int] = 1

    def urls(self) -> list[str]:
        return [
            RELEVANT_JOB_URL.format(page=page, per_page=self.per_page)
            for page in range(self.start, self.end + 1)
        ]


async def job_search(state: JobSearchState) -> None:

    html_schema = json.loads(RELEVANT_JOB_HTML_SCHEMA)
    browswer_config = BrowserConfig(
        storage_state=SESSION,
    )
    run_config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(
            html_schema
        ),
        stream=True,
        cache_mode=CacheMode.BYPASS,
    )

    writer = Writer(serialize, state.file)
    crawler = AsyncWebCrawler(config=browswer_config)
    crawler.crawler_strategy.set_hook("after_goto", after_goto)
    async with AuthAgent(url=LOGIN_URL) as auth:
        await auth.login()
    await crawler.start()
    writer.start()
    async for result in await crawler.arun_many(state.urls(), run_config):
        if not result.success:
            continue
        content = json.loads(result.extracted_content)
        await writer.write(content)
        if await writer.get_buffer_length() >= state.save_frequency:
            writer.flush()
    await writer.close()
    await crawler.close()


__all__ = ["deserialize", "JobSearchState", "job_search"]