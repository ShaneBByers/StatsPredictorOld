import requests
import json


class WebConnector:

    INIT_STRING = "https://statsapi.web.nhl.com/api/v1/"
    request_string = INIT_STRING

    def append_string(self, append_string):
        # logging.info("'" + self.request_string + "'  +  '" + append_string + "'")
        self.request_string += append_string
        # logging.info(self.request_string)

    def execute(self):
        # logging.info(self.request_string)
        content = requests.get(self.request_string).content
        self.request_string = self.INIT_STRING
        return json.loads(content)
