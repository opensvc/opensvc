def lazy(fn):
    """
    A decorator for on-demand initialization of a property
    """
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazyprop


def lazy_initialized(self, attr):
    """
    Return True if the lazy property has been initialized
    """
    attr_name = '_lazy_' + attr
    if hasattr(self, attr_name):
        return True
    return False


def set_lazy(self, attr, value):
    """
    Set a <value> as the <self> object lazy property hidden property value
    """
    attr_name = '_lazy_' + attr
    setattr(self, attr_name, value)


def unset_all_lazy(self):
    """
    Unset all lazy property hidden property, iow flush the cache
    """
    for attr in [attr for attr in self.__dict__]:
        if attr.startswith("_lazy_"):
            delattr(self, attr)


def unset_lazy(self, attr):
    """
    Unset <attr> lazy property hidden property, iow flush the cache
    """
    attr_name = '_lazy_' + attr
    if hasattr(self, attr_name):
        delattr(self, attr_name)



