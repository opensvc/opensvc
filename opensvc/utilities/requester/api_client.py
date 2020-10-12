import requests

import utilities.noop_log as noop_log


class ApiClient(object):
    def __init__(self,
                 auth_factory,
                 url_util,
                 log=noop_log):
        self._auth_factory = auth_factory
        self._url_util = url_util
        self.log = log

    def action(self, method='get', url=None, auth_info=None, headers=None, **kwargs):
        new_header = {}
        new_url = self._url_util.get_url(url)
        new_header.update(self._url_util.get_header(url))
        new_header.update(self._auth_factory.get_headers(auth_info or {}))
        new_header.update(headers or {})

        return getattr(requests, method)(url=new_url,
                                         headers=new_header,
                                         **kwargs)
