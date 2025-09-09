from typing import Any
from .utils import wait_to_be_visible_or_retry
from source import AuthAgent, Serializer
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

SAVE_FREQUENCY = 1 # flush buffer after # requests

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
    locator = page.get_by_role("button", name=re.compile(r"View.*")).first
    await wait_to_be_visible_or_retry(page, locator)
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


async def extract_relevant_jobs(start: int, end: int, per_page: int):
    storage_state = Path(os.getenv("SESSION_STORAGE")) / "handshake.json"
    storage = Path(os.getenv("STORAGE")) / "app_joinhandshake_com" / "apply"
    storage.mkdir(parents=True, exist_ok=True) #< create output directories if necessary.
    html_schema = json.loads(RELEVANT_JOB_HTML_SCHEMA)
    browswer_config = BrowserConfig(
        storage_state=storage_state,
        headless=True,
    )
    run_config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(
            html_schema
        ),
        stream=True,
        cache_mode=CacheMode.BYPASS,
    )
    urls = [
        RELEVANT_JOB_URL.format(page=page, per_page=per_page)
        for page in range(start, end + 1)
    ]
    serializer = Serializer(serialize, storage / 'p1.csv')
    crawler = AsyncWebCrawler(config=browswer_config)
    crawler.crawler_strategy.set_hook("after_goto", after_goto)
    async with AuthAgent(url=LOGIN_URL) as auth:
        await auth.login()
    await crawler.start()
    serializer.start()
    async for result in await crawler.arun_many(urls, run_config):
        if not result.success:
            continue
        content = json.loads(result.extracted_content)
        await serializer.write(content)
        if serializer.get_buffer_length() >= SAVE_FREQUENCY:
            serializer.flush()
    await serializer.close()
    await crawler.close()







"""
[
    {
        "jobs": [
            {
                "job_id": "/job-search/10266509?page=2&per_page=2",
                "position": "View Frontend Engineering Intern / Co-op (React + Next.js)",
                "href": "/job-search/10266509?page=2&per_page=2"
            },
            {
                "job_id": "/job-search/10260826?page=2&per_page=2",
                "position": "View Software Engineer (AI Agents)",
                "href": "/job-search/10260826?page=2&per_page=2"
            }
        ]
    },
    {
        "jobs": [
            {
                "job_id": "/job-search/10262893?page=1&per_page=2",
                "position": "View SQL Developer",
                "href": "/job-search/10262893?page=1&per_page=2"
            },
            {
                "job_id": "/job-search/10266791?page=1&per_page=2",
                "position": "View Data/AI Engineer Intern",
                "href": "/job-search/10266791?page=1&per_page=2"
            }
        ]
    }
]
"""