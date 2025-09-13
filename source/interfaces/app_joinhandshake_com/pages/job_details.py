from dataclasses import dataclass
from collections.abc import Callable
from typing import Any, Optional
from ..utilities.playwright_tools import wait_to_be_visible_or_retry
from source import (
    AuthAgent, 
    Writer, 
    Reader, 
    Cache,
    normalize_markdown
)
from datetime import (
    datetime, 
    timedelta
)
from dateutil.relativedelta import relativedelta
import json
import os
import re
import traceback
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
    RateLimiter,
    CrawlResult
)
from playwright.async_api import (
    Page,
    Locator,
    BrowserContext,
    expect
)

SESSION = Path(os.getenv("SESSION_STORAGE")) / "handshake.json"

CACHE_FILE = Path(os.getenv("STORAGE")) / "cache" / "app_joinhandshake_com" / "job_details.csv"

if not CACHE_FILE.exists():
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.touch(exist_ok=True)

LOGIN_URL = "https://uoregon.joinhandshake.com"

JOB_DETAIL_HTML_SCHEMA = \
"""
{
    "name": "Handshake Job Details",
    "baseSelector": "body",
    "fields": [
        {
            "name": "additional_documents",
            "selector": "input[placeholder*='Search your']",
            "type": "list",
            "fields": [
                {
                    "name": "document",
                    "type": "attribute",
                    "attribute": "placeholder"
                }
            ]
        },
        {
            "name": "company",
            "selector": "div[data-hook='job-details-page'] > div:first-child a:first-of-type > div",
            "type": "text"
        },
        {
            "name": "is_external_application",
            "selector": "div:first-of-type > button[aria-label*='Apply']",
            "type": "text"
        },
        {
            "name": "industry",
            "selector": "div[data-hook='job-details-page'] > div:first-child a:last-of-type > div",
            "type": "text"
        },
        {
            "name": "posted_date",
            "selector": "div[data-hook='job-details-page'] > div:first-of-type > div:last-of-type",
            "type": "text"
        },
        {
            "name": "deadline",
            "selector": "div[data-hook='job-details-page'] > div:first-of-type > div:last-of-type",
            "type": "text"
        },
        {
            "name": "pay",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(2) > div > div:first-of-type",
            "type": "text"
        },
        {
            "name": "location",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(3) > div > div:first-child",
            "type": "text"
        },
        {
            "name": "is_internship",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(4) > div > div:first-child",
            "type": "text"
        },
        {
            "name": "type",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(4) > div > div:last-child",
            "type": "text"
        },
        {
            "name": "duration",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(4) > div > div:last-child",
            "type": "text"
        },
        {
            "name": "error_detection",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(3) > div:nth-of-type(5) > div > div:last-child",
            "type": "text"
        },
        {
            "name": "overview",
            "selector": "div[data-hook='job-details-page'] > div:nth-of-type(4)",
            "type": "html"
        }
    ]
}
"""


def _additional_documents(documents: list[dict[str, str]]) -> list[str]:
    result = []
    pattern = r"Search your (\w+)"
    for item in documents:
        match = re.search(pattern, item['document'])
        if not match:
            raise Exception("Error: couldn't match document type in required document") 
        result.append(match.group(1))
    return result


def _is_external_application(text: str) -> bool:
    return text == "Apply externally"


def _posted_date(text: str) -> int:
    pattern = r"posted (\d+) (\w+) ago"
    match = re.match(pattern, text, re.IGNORECASE)
    if not match:
        return datetime.now().strftime(r"%Y-%m-%d")
    value, unit = int(match.group(1)), match.group(2).lower()
    if not unit.endswith('s'):
        unit += "s"
    kwargs = {unit: value}
    posted_date = datetime.now() - relativedelta(**kwargs)
    return int(posted_date.timestamp())


def _deadline(text: str) -> int:
    pattern = r"Apply by (.*)"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        # No deadline -> return the max INT32 POSIX timestamp (~2038).
        return 2**31 - 1
    deadline_date = datetime.strptime(match.group(1), "%B %d, %Y at %I:%M %p")
    return int(deadline_date.timestamp())


def _pay(text: str) -> str:
    if text == 'Paid':
        return 'N/A'
    if text == "":
        return '0/yr'
    unit_case_one = re.search(r"\/(.*)", text)
    unit_case_two = re.search(r"per (\w+)", text)
    if not unit_case_one and not unit_case_two:
        raise Exception(f"Error: couldn't match unit in pay (text: {text})")
    if unit_case_one:
        unit = unit_case_one.group(1)
    else:
        unit = unit_case_two.group(1)
    if unit == 'yr' or unit == 'year':
        factor = 1 
    elif unit == 'mo' or unit == 'month':
        factor = 12
    elif unit == 'wk' or unit == 'week':
        factor = 52
    elif unit == 'hr' or unit == 'hour':
        factor = 40 * 52
    in_thousands = re.search(r"(K)\/.*", text)
    if in_thousands:
        factor *= 1000
    wage_range = [int(wage) for wage in re.findall(r'\d+', text)]
    annual_wage_range = [f'{int(wage) * factor:,}' for wage in wage_range]
    annual_wage_string = '$' + '-'.join(annual_wage_range) + '/yr'
    return annual_wage_string
    

def _is_internship(text: str) -> bool:
    return text == "Internship"


def _type(text: str) -> str:
    return text.lower()


def _duration(text: str) -> str:
    match = re.search(r"From (.*)", text)
    if not match:
        return "N/A"
    return match.group(1)


def clean_extracted_content(content: dict[str, Any]):
    content.update({
        'additional_documents': _additional_documents(content['additional_documents']),
        'is_external_application': _is_external_application(content['is_external_application']),
        'posted_date': _posted_date(content['posted_date']),
        'deadline': _deadline(content['deadline']),
        'pay': _pay(content['pay']),
        'is_internship': _is_internship(content['is_internship']),
        'type': _type(content['type']),
        'duration': _duration(content['duration']),
        'overview': normalize_markdown(content['overview'])
    })


def serialize(buffer: list[Any]) -> list[str]:
    serialized_buffer = []
    for item in buffer:
        serialized_buffer.append([
            item['job_id'], item['position'], item['url'],
            json.dumps(item['additional_documents']), item['company'],
            json.dumps(item['is_external_application']), item['industry'],
            item['posted_date'], item['deadline'], item['pay'],
            json.dumps(item['is_internship']), item['type'], item['duration'],
            item['location'], json.dumps(item['overview']),
        ])
    return serialized_buffer


def deserialize(row: list[str]) -> dict[str, Any]:
    return {
        'job_id': row[0], 'position': row[1], 'url': row[2], 'additional_documents': json.loads(row[3]), 'company': row[4],        
        'is_external_application': json.loads(row[5]), 'industry': row[6], 'posted_date': row[7], 'deadline': row[8], 'pay': row[9],
        'is_internship': json.loads(row[10]), 'type': row[11], 'duration': row[12], 'location': row[13], 'overview': json.loads(row[14]),
    }


async def after_goto(page: Page, context: BrowserContext, url: str, response, **kwargs):
    url_pattern = r'^.*/jobs/.*$'
    apply_button = page.get_by_role("button", name=re.compile("apply", re.IGNORECASE)).first
    await wait_to_be_visible_or_retry(page, apply_button, url_pattern=url_pattern)
    more_button = page.get_by_role("button", name="More").first
    if await more_button.is_visible():
        await more_button.click()
    return page


@dataclass
class JobDetailsState:
    file_input: Path
    deserialize: Callable[[list[str]], list[str]]
    file_output: Path
    batch_size: Optional[int] = 50
    save_frequency: Optional[int] = 1


async def job_details(state: JobDetailsState) -> None:
    html_schema = json.loads(JOB_DETAIL_HTML_SCHEMA)
    browswer_config = BrowserConfig(
        storage_state=SESSION,
        headless=True
    )
    run_config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(
            html_schema
        ),
        stream=True,
    )
    markdown_config = CrawlerRunConfig(
        markdown_generator=DefaultMarkdownGenerator()
    )
    cache = Cache(
        reader=Reader(CACHE_FILE, 1024, deserialize),
        writer=Writer(CACHE_FILE, serialize)
    )
    crawler = AsyncWebCrawler(config=browswer_config)
    crawler.crawler_strategy.set_hook("after_goto", after_goto)
    reader = Reader(state.file_input, state.batch_size, state.deserialize)
    writer = Writer(state.file_output, serialize)
    write_from_cache = Writer(state.file_output, serialize)
    write_from_cache.start()
    cache_misses = []
    async for batch in reader:
        fields = ['job_id']
        queries = [(item['job_id'],) for item in batch]
        misses = await cache.query_cache(write_from_cache, fields, queries)
        cache_misses.extend([query[0] for query in misses])
        write_from_cache.flush()
    await write_from_cache.close()
    job_map = {}
    async for batch in reader:
        job_map.update({job['url']: job for job in batch if job['job_id'] in cache_misses})
    async with AuthAgent(url=LOGIN_URL) as auth:
        await auth.login()
    await crawler.start()
    writer.start(overwrite=False)
    cache.start_write()
    async for result in await crawler.arun_many(job_map.keys(), run_config):
        if not result.success:
            continue
        try:
            content = json.loads(result.extracted_content)[0]
            if not content.get('error_detection', None):
                print("\x1b[31m[ERROR]...   most likely missing 'pay' information\x1b[0m")
                continue
            markdown: CrawlResult = await crawler.arun(f'raw://{content['overview']}', config=markdown_config)
            if not markdown.success:
                print('\x1b[31;1mError generating markdown\x1b[0m')
                continue
            clean_extracted_content(content)
            content.update({
                'job_id': job_map[result.url]['job_id'],
                'position': job_map[result.url]['position'],
                'url': job_map[result.url]['url'],
                'overview': markdown.markdown
            })
            await writer.write([content])
            await cache.write([content])
            if await writer.get_buffer_length() >= state.save_frequency:
                writer.flush()
                cache.flush()
        except Exception as e:
            traceback.print_exc()
            print(f' \x1b[31mCrawl Error: {e!r}\x1b[0m')
    await writer.close()
    await cache.close_write()
    await crawler.close()
    