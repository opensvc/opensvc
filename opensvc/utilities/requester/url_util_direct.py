from utilities.requester.url_util_abstract import UrlUtilAbstract


class UrlUtilDirect(UrlUtilAbstract):
    def get_header(self, url):
        return {}

    def get_url(self, url):
        return url
