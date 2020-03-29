def fcache(fn):
    """
    A decorator for caching the result of a function
    """
    attr_name = '_fcache_' + fn.__name__

    def _fcache(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _fcache


def fcache_initialized(self, attr):
    """
    Return True if the function has already been cached
    """
    attr_name = '_fcache_' + attr
    if hasattr(self, attr_name):
        return True
    return False


def unset_fcache(self, attr):
    """
    Unset <attr> function cache
    """
    attr_name = '_fcache_' + attr
    if hasattr(self, attr_name):
        delattr(self, attr_name)

