from bs4 import BeautifulSoup, Tag
import re
import functools


def return_none_on_attribute_error(func):
    """
    This makes accessing the parent tag, sibling tags, or etc from a potentially
    non-existent tag safer by returning None instead of raising an error.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except AttributeError:
            return None
    return wrapper


class HandshakeRawDataContainer:
    MONEY_SVG_D = re.compile(rf'^{re.escape("M2.5 8C2.22386")}')
    LOCATION_SVG_D = re.compile(rf'^{re.escape("M12 2C15.866")}')
    JOB_SVG_D = re.compile(rf'^{re.escape("M11.5527 2.72314")}')

    def __init__(self, html) -> None:
        self.soup = BeautifulSoup(html, 'html.parser')

    @return_none_on_attribute_error
    def get_wage(self) -> str | None:
        """ Orients around the money SVG icon to navigate to the wage. """
        return self.soup.find('path', {'d': self.MONEY_SVG_D}).parent.find_next_sibling('div').div.get_text()
    
    @return_none_on_attribute_error
    def get_location(self) -> str | None:
        """ Orients around the location SVG icon to navigate to the location. """
        return self.soup.find('path', {'d': self.LOCATION_SVG_D}).parent.find_next_sibling('div').div.get_text()

    @return_none_on_attribute_error
    def get_employment_type(self) -> str | None:
        """ Orients around the job SVG icon to navigate to the employment type. """
        return self.soup.find('path', {'d': self.JOB_SVG_D}).parent.find_next_sibling('div').find_all('div')[1].get_text()

    @return_none_on_attribute_error
    def get_job_type(self) -> str | None:
        """ Orients around the job SVG icon to navigate to the job type. """
        return self.soup.find('path', {'d': self.JOB_SVG_D}).parent.find_next_sibling('div').find_all('div')[0].get_text()

    @return_none_on_attribute_error
    def get_about(self) -> str | None:
        """ Orients around 'at a glance' title to navigate to the about section. """
        return self.soup.find('h3', string="At a glance").parent.parent.find_next_sibling('div').div.div.decode_contents()
        
    @return_none_on_attribute_error
    def get_apply_type(self) -> str | None:
        """ Gets the text from the 'Apply' or 'Apply externally' button. """
        return self.soup.find('button', {'aria-label': re.compile('Apply')}).get_text()

    def _get_position_tag(self) -> Tag | None:
        """ Selects the tag that contains the position name through a (rather tricky) anchor tag relationship. """
        return self.soup.select_one('a[href^="/jobs/"][href*="?searchId="] h1')

    @return_none_on_attribute_error
    def get_position(self) -> str | None:
        """ Gets the text from the position tag """
        return self._get_position_tag().get_text()

    @return_none_on_attribute_error
    def get_times(self) -> str | None:
        """ Orients around the position tag to navigate to the line that contains 'posted' and 'apply by' dates. """
        return self._get_position_tag().parent.find_next_sibling('div').get_text()

    @return_none_on_attribute_error
    def get_company(self) -> str | None:
        """ Orients around the position tag to navigate to the company name. """
        return self._get_position_tag().parent.find_previous_sibling('div').div.find_all('a')[0].div.get_text()
    
    @return_none_on_attribute_error
    def get_industry(self) -> str | None:
        """ Orients around the position tag to navigate to the industry name. """
        return self._get_position_tag().parent.find_previous_sibling('div').div.find_all('a')[1].div.get_text()
        
    def get_documents(self) -> list[str]:
        """ Selects all anchor tags that have a placeholder which contain 'search your' (case insensitive) """
        """ Then, returns a list containing the placeholder attributes. """
        return list(map(lambda doc: doc.attrs['placeholder'], self.soup.select("input[placeholder*='search your'i]")))

    def get_all(self):
        return \
        {
            'wage': self.get_wage(),
            'location': self.get_location(),
            'employment_type': self.get_employment_type(),
            'job_type': self.get_job_type(),
            'about': self.get_about(),
            'apply_type': self.get_apply_type(),
            'position': self.get_position(),
            'times': self.get_times(),
            'company': self.get_company(),
            'industry': self.get_industry(),
            'documents': self.get_documents()
        }
