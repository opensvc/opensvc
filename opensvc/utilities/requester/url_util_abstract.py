class UrlUtilAbstract(object):
    def get_url(self, url):
        raise NotImplementedError

    def get_header(self, url):
        raise NotImplementedError
