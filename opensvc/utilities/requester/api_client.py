import requests


class ApiClient:
    def __init__(self,
                 auth_factory,
                 url_util):
        self._auth_factory = auth_factory
        self._url_util = url_util

    def action(self, method, url, auth_info=None, headers=None, *args, **kwargs):
        new_header = {}
        new_url = self._url_util.get_url(url)
        new_header.update(self._url_util.get_header(url))
        new_header.update(self._auth_factory.get_header(auth_info or {}))
        new_header.update(headers or {})

        return getattr(requests, method)(url=new_url,
                                         headers=new_header,
                                         *args,
                                         **kwargs)
