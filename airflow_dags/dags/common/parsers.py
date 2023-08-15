import logging
from html.parser import HTMLParser as BaseHtmlParser
from typing import Dict
from typing import List
from typing import Optional

logger = logging.getLogger(__name__)


class HTMLParser(BaseHtmlParser):

    def __init__(self, output_list: Optional[List] = None) -> None:
        BaseHtmlParser.__init__(self)
        if output_list is None:
            self.output_list = []
        else:
            self.output_list = output_list

    def handle_starttag(self, tag: str, attrs: Dict[str, str]) -> None:
        if tag == 'a':
            self.output_list.append(dict(attrs).get('href'))

    def error(self, message: str) -> None:
        logger.error(f'Get error during feed parsing: {message}')
