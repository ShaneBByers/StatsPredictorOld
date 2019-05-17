from html.parser import HTMLParser

import logging


class SalaryDataCollector(HTMLParser):

    def __init__(self, logger_name):
        HTMLParser.__init__(self)
        self.logger = logging.getLogger(logger_name)
        self.page_id = ""

    def error(self, message):
        self.logger.error(message)

    def handle_starttag(self, tag, attrs):
        # self.logger.info("Encountered a start tag: " + tag)
        if self.page_id == "":
            found = False
            for attr in attrs:
                if attr[0] == "class" and "contest_list__item--first" in attr[1]:
                    found = True
            if found:
                for attr in attrs:
                    if attr[0] == "data-id":
                        self.page_id = attr[1]
