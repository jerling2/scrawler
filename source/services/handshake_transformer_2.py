import re
import json
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
from typing import Optional, Any
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, JsonCssExtractionStrategy, CacheMode
from source.broker import InterProcessGateway, IPGConsumer
from source.codec import HandshakeTransformer2Codec, HandshakeLoader1Codec
from source.crawlers import CrawlerFactory, CrawlerFactoryConfig
from source.database import HandshakeRepoT2


@dataclass
class HandshakeTransformer2Config:
    source_topics = ['raw.handshake.job.stage2.v1']
    codec = HandshakeTransformer2Codec

    def get_crawler(self) -> AsyncWebCrawler:
        return CrawlerFactory(
            CrawlerFactoryConfig(
                browser_config=BrowserConfig(
                    headless=True,
                ),
                hooks={}
            )
        ).create_crawler()


class HandshakeTransformer2:

    def __init__(
        self, 
        config: HandshakeTransformer2Config,
        broker: InterProcessGateway,
        repo: HandshakeRepoT2
    ) -> None:
        self.config = config
        self.broker = broker
        self.repo = repo
        self.crawler = config.get_crawler()

    @property
    def extraction_strategy(self) -> JsonCssExtractionStrategy:
        return JsonCssExtractionStrategy({
            'baseSelector': 'body',
            'fields': [
                {
                    "name": "documents",
                    "selector": "input[placeholder*='search your'i]",
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
                    'name': 'at_a_glance',
                    'selector': 'div[data-hook="job-details-page"] > div:nth-of-type(4) > div',
                    'type': 'list',
                    'fields': [
                        {
                            'name': 'row',
                            'type': 'text'
                        }
                    ]
                },
                {
                    'name': 'about_section',
                    'selector': 'div[data-hook="job-details-page"] > div:nth-of-type(5)',
                    'type': 'html'
                },
                {
                    'name': 'company',
                    'selector': 'div[data-hook="job-details-page"] > div:first-of-type > div > div > a:first-of-type',
                    'type': 'text'
                },
                {
                    'name': 'industry',
                    'selector': 'div[data-hook="job-details-page"] > div:first-of-type > div > div > a:last-of-type',
                    'type': 'text'
                },
                {
                    'name': 'role',
                    'selector': 'div[data-hook="job-details-page"] > div:first-of-type h1',
                    'type': 'text'
                },
                {
                    'name': 'time',
                    'selector': 'div[data-hook="job-details-page"] > div:first-of-type > div:last-of-type',
                    'type': 'text'
                },
                {
                    'name': 'apply',
                    'selector': 'button[aria-label*="apply"i]',
                    'type': 'list',
                    'fields': [
                        {
                            'name': 'button',
                            'type': 'text'
                        }
                    ]
                }
            ]
        })

    @property
    def consumer_info(self) -> IPGConsumer:
        return IPGConsumer(
            topics=self.config.source_topics,
            codec=self.config.codec,
            notify=self.on_notify
        )

    def on_notify(self, message: HandshakeTransformer2Codec):
        match message.action:
            case 'START_TRANSFORM':
                asyncio.run(self.transform(message.url, message.html, message.created_at))
            case _:
                pass
        return

    def process(self, url: str, content: dict[str, str], created_at: datetime) -> HandshakeLoader1Codec:
        output = {}
        glance = ProcessAtAGlance.process(content['at_a_glance'])
        output['url'] = url
        output['overview'] = process_about_markdown(content['about_md'])
        output['documents'] = process_documents(content['documents'])
        output['company'] = lower_and_strip(content['company'])
        output['industry'] = lower_and_strip(content['industry'])
        output['role'] = lower_and_strip(content['role'])
        output['is_internal_apply'] = process_apply(content['apply'])
        output['posted_at'], output['apply_by'] = process_posted_at(content['time'], created_at)
        output['wage'] = glance['wage']
        output['location_type'] = glance['location_type']
        output['location'] = glance['location']
        output['job_type'] = glance['job_type']
        output['is_internship'] = glance['is_internship']
        message = HandshakeLoader1Codec(**output)
        return message
    
    async def transform(self, url: str, html: str, created_at: datetime):
        config = CrawlerRunConfig(
            extraction_strategy=self.extraction_strategy,
            cache_mode=CacheMode.BYPASS,
        )
        await self.crawler.start()
        result = await self.crawler.arun(f'raw:{html}', config)
        if not result.success:
            await self.crawler.close()
            return
        content = json.loads(result.extracted_content)[0]
        about_html = content['about_section']
        markdown = await self.crawler.arun(f'raw:{about_html}')
        if not markdown.success:
            await self.crawler.close()
            return
        await self.crawler.close()
        content['about_md'] = markdown.markdown
        msg = self.process(url, content, created_at)
        self.repo.insert(
            msg.overview,
            msg.posted_at,
            msg.apply_by,
            msg.documents,
            msg.company,
            msg.industry,
            msg.role,
            msg.is_internal_apply,
            msg.wage,
            msg.location_type,
            msg.location,
            msg.job_type,
            msg.is_internship
        )
        self.broker.send(HandshakeLoader1Codec, HandshakeLoader1Codec.TOPIC, msg)
        pass

class ProcessAtAGlance:

    @classmethod
    def _normalize_row(cls, raw_row: str) -> str:
        NORM_ROWS_PATTERN = r'((?<!^)(?:(?<!\d|[A-Z]|\s)K|(?<!\s|[A-Z])[A-JL-Z])|(?<=[A-Z][A-Z])[A-Z])'
        EM_DASH = '\\xe2\\x80\\x93'
        BULLET = '\\xe2\\x88\\x99'
        # normalize dash
        clean_row = raw_row.replace(EM_DASH, '-')
        # remove bullet point
        clean_row = clean_row.replace(BULLET, ' ')
        # add add spaces
        clean_row = re.sub(NORM_ROWS_PATTERN, r' \1', clean_row)
        # lowercase and remove trailing and leading whitespace
        clean_row = clean_row.lower().strip()
        return clean_row        

    @classmethod
    def _normalize_wage(cls, unit: str, in_thousands: bool, start: str, end: Optional[str]=None) -> list[int, int]:
        HOURS_PER_YEAR = 40 * 52
        WEEKS_PER_YEAR = 52
        MONTHS_PER_YEAR = 12
        k = 1000 if in_thousands else 1
        wage = [int(start), int(end) if isinstance(end, str) else int(start)]
        annual_wage = lambda per_year, k, wage: list(map(lambda x: x * per_year * k, wage))
        if (unit == 'hr' or unit == 'hour') and in_thousands:
            # I'm guessing they meant annual wage, and not thousands of dollars per hour.
            return annual_wage(1, k, wage)
        if unit == 'hr' or unit == 'hour':
            return annual_wage(HOURS_PER_YEAR, 1, wage)
        if unit == 'wk' or unit == 'week':
            return annual_wage(WEEKS_PER_YEAR, k, wage)
        if unit == 'mo' or unit == 'month':
            return annual_wage(MONTHS_PER_YEAR, k, wage)
        if unit == 'yr' or unit == 'year':
            return annual_wage(1, k, wage)
        raise ValueError(f'Unexpected unit: {unit}')

    @classmethod
    def _get_wage(cls, output: dict[str, Any], clean_row: str) -> bool:
        """
        Returns True if wage was present in this line, else False.
        Not all jobs on Handshake post a wage, but if they do, it's
        usually in the second row.
        """
        OUTPUT_KEY = 'wage'
        UNIT_PATTERN = r'(?<=\/)(\w+)|(?<=per )(\w+)|(paid)|(unpaid)'
        WAGE_PATTERN =  r'.*?(\d+)(?:[^\d].*?(\d+))?'
        K_PATTERN = r'(?<=\d)(k)'
        match_unit = re.search(UNIT_PATTERN, clean_row)
        match_wage = re.search(WAGE_PATTERN, clean_row)
        match_k = re.search(K_PATTERN, clean_row)
        in_thousands = True if match_k else False
        unit = match_unit.group() if match_unit else None
        wage = match_wage.groups() if match_wage else None
        if unit is None:
            # This job did not list a wage. 
            # E.g. no paid, unpaid, specific wage, or a wage range.
            output[OUTPUT_KEY] = None
            return False 
        if unit == 'unpaid':
            output[OUTPUT_KEY] = [0, 0]
            return True
        if unit == 'paid':
            output[OUTPUT_KEY] = None
            return True
        output[OUTPUT_KEY] = cls._normalize_wage(unit, in_thousands, *wage)
        return True

    @classmethod
    def _get_location(cls, output: dict[str, Any], clean_row: str) -> bool:
        """
        Returns True, unless there's an error, then raises a ValueError.
        I believe every job posting on Handshake MUST include a location.
        """
        LOC_TYPE_PATTERN =  r'(onsite|remote|hybrid)'
        LOCATION_PATTERN = r'(?<=based in )(.*)(?= work)'
        match_type = re.search(LOC_TYPE_PATTERN, clean_row)
        match_location = re.search(LOCATION_PATTERN, clean_row)
        location_type = [t for t in match_type.groups()] if match_type else None
        location = match_location.group() if match_location else None
        if location_type is None:
            raise ValueError('No location type specified (e.g. onsite, remote, or hybrid)')
        output['location_type'] = location_type
        output['location'] = location
        return True

    @classmethod
    def _get_job(cls, output: dict[str, Any], clean_row: str) -> bool:
        """
        Returns True. Every job posting should have a job type and disclose
        whether or not it's a internship.
        """
        JOB_TYPE_PATTERN = r'\w+-time'
        match_type = re.search(JOB_TYPE_PATTERN, clean_row)
        job_type = match_type.group() if match_type else None
        is_internship = 'internship' in clean_row
        output['job_type'] = job_type
        output['is_internship'] = is_internship
        return True 

    @classmethod
    def process(cls, at_a_glance_data: list[dict[str, str]]) -> dict[str, Any]:
        """
        Stages are ordered by how the information is layout on Handshake's UI.
        Sometimes the wage isn't posted, in which the stage is skipped via the stack.
        """
        STAGES = [cls._get_wage, cls._get_location, cls._get_job]
        num_stages = len(STAGES)
        stack = [cls._normalize_row(row['row']) for row in at_a_glance_data]
        if not len(stack):
            raise ValueError('No rows collected')
        first_clean_row = stack.pop(0)
        if first_clean_row != 'at a glance':
            raise ValueError("Expected to see 'at a glance' as the first row")
        output = {}
        step = 0
        while step < num_stages and len(stack):
            clean_row = stack.pop(0)
            while step < num_stages and not STAGES[step](output, clean_row):
                # this is a weird implementation (I admit). The only stage that can return
                # false is _get_wage because sometimes the wage isn't posted. In that case,
                # the same `clean_row` is tried again with the next stage. This allows the
                # flexibility of defining stages that may not exist on the Handshake UI, but
                # if they do exist, then it will be in some predicitable order.
                step += 1
            step += 1
        return output


def process_documents(documents: list[dict[str, str]]) -> list[str]:
    DOCUMENT_PATTERN = r'(?<=Search your\s)(?:.*[^s]|)'
    clean_docs = [document['document'].strip() for document in documents]
    out_docs = [
        re.search(DOCUMENT_PATTERN, doc).group()
        for doc in clean_docs
        if re.search(DOCUMENT_PATTERN, doc)
    ]
    return out_docs


def process_about_markdown(about_md: str) -> str:
    HEX_PATTERN = r'\\(x..)'
    EXTRA_NEWLINES_PATTERN = r'(?<=\n)(?:\s|\n)+'
    EXTRA_SPACES_PATTERN = r'(?<=\s)\s+'
    LESS_PATTERN = r'Less\n'
    # c4ai or playwright doesn't decode these?
    md = about_md.replace(r'\\xc2\\xa0', ' ')
    md = md.replace(r'\\xe2\\x80\\x93', '-')
    md = md.replace(r'\\xe2\\x80\\x98', '"')
    md = md.replace(r'\\xe2\\x80\\x99', '"')
    md = re.sub(LESS_PATTERN, '', md)
    md = re.sub(HEX_PATTERN, '', md)
    md = re.sub(EXTRA_NEWLINES_PATTERN, '', md)
    md = re.sub(EXTRA_SPACES_PATTERN, '', md)
    md = md.strip()
    return md


def process_posted_at(raw: str, created_at: datetime) -> list[datetime]:
    clean = raw.replace('\\xe2\\x88\\x99', ' ')
    POSTED_PATTERN = r'(?<=posted )(\d+) (\w+)(?<!s)'
    APPLY_PATTERN = r'(?<=apply by )(\w+) (\d+), (\d+) at (\d+:\d+) (am|pm)'
    match_posted = re.search(POSTED_PATTERN, clean, re.IGNORECASE)
    match_apply = re.search(APPLY_PATTERN, clean, re.IGNORECASE)
    t_delta, t_unit = match_posted.groups()
    delta = {t_unit + 's': int(t_delta)}
    posted_at = created_at - relativedelta(**delta)
    apply_by = datetime.strptime('{}{}{}{}{}'.format(*match_apply.groups()), "%B%d%Y%I:%M%p")
    return posted_at, apply_by


def process_apply(apply_buttons: list[dict[str, str]]) -> bool:
    if not len(apply_buttons):
        raise ValueError('was expecting buttons')
    button_string = apply_buttons[0]['button']
    is_internal_apply = button_string == 'Apply'
    return is_internal_apply


def lower_and_strip(text: str) -> str:
    return text.lower().strip()

