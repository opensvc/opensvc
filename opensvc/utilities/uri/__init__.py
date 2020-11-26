import os

from contextlib import contextmanager

try:
    from foreign.six.moves.urllib.request import Request, urlopen
except ImportError:
    # pylint false positive
    pass

class Uri(object): 
    def __init__(self, uri, secure=True): 
        self.uri = uri 
        self.secure = secure
      
    @contextmanager
    def fetch(self):
        fpath = self._fetch_path()
        try:
            self._fetch(self.uri, fpath)
            yield fpath
        finally:
            try:
                os.unlink(fpath)
            except OSError:
                pass

    @staticmethod
    def _fetch_path():
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        fpath = tmpf.name
        tmpf.close()
        return fpath

    def _fetch(self, uri, fpath):
        """
        A chunked download method
        """
        request = Request(uri)
        kwargs = self._set_ssl_context()
        ufile = urlopen(request, **kwargs)
        with open(fpath, 'wb') as ofile:
            os.chmod(fpath, 0o0600)
            for chunk in iter(lambda: ufile.read(4096), b""):
                ofile.write(chunk)
        ufile.close()

    def _set_ssl_context(self, kwargs=None):
        """
        Python 2.7.9+ verifies certs by default and support the creationn
        of an unverified context through ssl._create_unverified_context().
        This method add an unverified context to a kwargs dict, when
        necessary.
        """
        kwargs = kwargs or {}
        if not self.secure:
            try:
                import ssl
                kwargs["context"] = ssl._create_unverified_context()
            except (ImportError, AttributeError):
                pass
        return kwargs

