class UrlUtilAbstract:
    def get_url(self, url: str) -> str:
        raise NotImplemented

    def get_header(self, url: str) -> str:
        raise NotImplemented
