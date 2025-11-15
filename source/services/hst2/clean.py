import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Protocol, Optional, TypedDict
from markdownify import markdownify as md


class HandshakeRawDataContainer(Protocol):

    def get_about(self) -> str | None:
        ...
    
    def get_apply_type(self) -> str | None:
        ...
    
    def get_company(self) -> str | None:
        ...

    def get_documents(self) -> list[str]:
        ...
    
    def get_employment_type(self) -> str | None:
        ...

    def get_industry(self) -> str | None:
        ...

    def get_job_type(self) -> str | None:
        ...

    def get_location(self) -> str | None:
        ...

    def get_position(self) -> str | None:
        ...
    
    def get_times(self) -> str | None:
        ...

    def get_wage(self) -> str | None:
        ...


class HandshakeCleanData(TypedDict):
    about: str | None
    apply_by: datetime | None
    apply_type: str | None
    company: str | None
    documents: list[str]
    employment_type: str | None
    industry: str | None
    job_type: str | None
    location: str | None
    location_type: list[str]
    position: str | None
    posted_at: datetime | None
    wage: list[int, int] | None


class HandshakeCleanDataContainer:

    def __init__(self, raw_html_container: HandshakeRawDataContainer, extracted_on: datetime) -> None:
        self.html = raw_html_container
        self.extracted_on = extracted_on

    def _lower_and_strip(self, raw: str) -> str:
        return raw.lower().strip()

    def _normalize(self, raw: str) -> str:
        NORM_ROWS_PATTERN = r'((?<!^)(?:(?<!\d|[A-Z]|\s)K|(?<!\s|[A-Z])[A-JL-Z])|(?<=[A-Z][A-Z])[A-Z])'
        EM_DASH = '\\xe2\\x80\\x93'
        BULLET = '\\xe2\\x88\\x99'
        # normalize dash
        step_1 = raw.replace(EM_DASH, '-')
        # remove bullet point
        step_2 = step_1.replace(BULLET, ' ')
        # add add spaces
        step_3 = re.sub(NORM_ROWS_PATTERN, r' \1', step_2)
        # lowercase and remove trailing and leading whitespace
        result = step_3.lower().strip()
        return result     

    def _convert_to_annual_wage(self, unit: str, in_thousands: bool, start: int, end: Optional[int] = None):
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

    def get_wage(self) -> list[int, int] | None:
        raw_wage = self.html.get_wage()
        if raw_wage is None:
            # There's no mention of paid, unpaid, specific wage, or a wage range.
            return None
        clean_wage = self._normalize(raw_wage)
        UNIT_PATTERN = r'(?<=\/)(\w+)|(?<=per )(\w+)|(paid)|(unpaid)'
        WAGE_PATTERN =  r'.*?(\d+)(?:[^\d].*?(\d+))?'
        K_PATTERN = r'(?<=\d)(k)'
        raw_wage = self.html.get_wage()
        clean_wage = self._normalize(raw_wage)
        match_unit = re.search(UNIT_PATTERN, clean_wage)
        match_wage = re.search(WAGE_PATTERN, clean_wage)
        match_k = re.search(K_PATTERN, clean_wage)
        in_thousands = True if match_k else False
        unit = match_unit.group() if match_unit else None
        wage = match_wage.groups() if match_wage else None
        if unit == 'unpaid':
            return [0, 0]
        if unit == 'paid':
            return None
        return self._convert_to_annual_wage(unit, in_thousands, *wage)
    
    def get_location(self) -> str | None:
        raw_location = self.html.get_location()
        if raw_location is None:
            return None
        clean_location = self._normalize(raw_location)
        LOCATION_PATTERN = r'(?<=based in )(.*)$'
        match_location = re.search(LOCATION_PATTERN, clean_location)
        location = match_location.group() if match_location else None
        return location

    def get_location_type(self) -> list[str]:
        raw_location = self.html.get_location()
        if raw_location is None:
            return []
        clean_location = self._normalize(raw_location)
        LOC_TYPE_PATTERN =  r'(onsite|remote|hybrid)'
        match_type = re.search(LOC_TYPE_PATTERN, clean_location)
        location_type = [t for t in match_type.groups()] if match_type else None
        return location_type

    def get_employment_type(self) -> str | None:
        raw_employment_type = self.html.get_employment_type()
        if raw_employment_type is None:
            return None
        clean_employment_type = self._normalize(raw_employment_type)
        JOB_TYPE_PATTERN = r'\w+-time'
        match_type = re.search(JOB_TYPE_PATTERN, clean_employment_type)
        employment_type = match_type.group() if match_type else None
        return employment_type
    
    def get_job_type(self) -> str | None:
        raw_job_type = self.html.get_job_type()
        if raw_job_type is None:
            return None
        clean_job_type = self._normalize(raw_job_type)
        return clean_job_type

    def get_about(self):
        raw_about = self.html.get_about()
        if raw_about is None:
            return None
        md_about = md(raw_about)
        return md_about
    
    def get_apply_type(self) -> str | None:
        raw_apply_type = self.html.get_apply_type()
        if raw_apply_type is None:
            return None
        clean_apply_type = self._normalize(raw_apply_type)
        return 'internal' if clean_apply_type == 'apply' else 'external'
    
    def get_position(self) -> str | None:
        raw_position = self.html.get_position()
        if raw_position is None:
            return None
        clean_position = self._lower_and_strip(raw_position)
        return clean_position
    
    def get_posted_at(self) -> datetime | None:
        raw_times = self.html.get_times()
        if raw_times is None:
            return None
        clean_times = raw_times.replace('\\xe2\\x88\\x99', ' ')
        POSTED_PATTERN = r'(?<=posted )(\d+) (\w+)(?<!s)'
        match_posted = re.search(POSTED_PATTERN, clean_times, re.IGNORECASE)
        t_delta, t_unit = match_posted.groups()
        delta = {t_unit + 's': int(t_delta)}
        posted_at = self.extracted_on - relativedelta(**delta)
        return posted_at
    
    def get_apply_by(self) -> datetime | None:
        raw_times = self.html.get_times()
        if raw_times is None:
            return None
        clean_times = raw_times.replace('\\xe2\\x88\\x99', ' ')
        APPLY_PATTERN = r'(?<=apply by )(\w+) (\d+), (\d+) at (\d+:\d+) (am|pm)'
        match_apply = re.search(APPLY_PATTERN, clean_times, re.IGNORECASE)
        apply_by = datetime.strptime('{}{}{}{}{}'.format(*match_apply.groups()), "%B%d%Y%I:%M%p")
        return apply_by

    def get_company(self) -> str | None:
        raw_company = self.html.get_company()
        if raw_company is None:
            return None
        clean_company = raw_company.strip()
        return clean_company
    
    def get_industry(self) -> str | None:
        raw_industry = self.html.get_industry()
        if raw_industry is None:
            return None
        clean_industry = self._lower_and_strip(raw_industry)
        return clean_industry
    
    def get_documents(self) -> list[str]:
        DOCUMENT_PATTERN = r'(?<=Search your\s)(?:.*[^s]|)'
        raw_documents = self.html.get_documents()
        clean_documents = [raw_doc.strip() for raw_doc in raw_documents]
        documents = [
            re.search(DOCUMENT_PATTERN, doc).group()
            for doc in clean_documents
            if re.search(DOCUMENT_PATTERN, doc)
        ]
        return documents
    
    def get_all(self) -> HandshakeCleanData:
        return {
            'about': self.get_about(),
            'apply_by': self.get_apply_by(),
            'apply_type': self.get_apply_type(),
            'company': self.get_company(),
            'documents': self.get_documents(),
            'employment_type': self.get_employment_type(),
            'industry': self.get_industry(),
            'job_type': self.get_job_type(),
            'location': self.get_location(),
            'location_type': self.get_location_type(),
            'position': self.get_position(),
            'posted_at': self.get_posted_at(),
            'wage': self.get_wage(),
        }
