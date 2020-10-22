from threading import RLock

_missing = object()

class threadsafe_lazy(object):
    def __init__(self, func):
        self.__name__ = "_lazy_" + func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self.func = func
        self.lock = RLock()

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        with self.lock:
            value = obj.__dict__.get(self.__name__, _missing)
            if value is _missing:
                value = self.func(obj)
                obj.__dict__[self.__name__] = value
            return value

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
    return hasattr(self, attr_name)


def set_lazy(self, attr, value):
    """
    Set a <value> as the <self> object lazy property hidden property value
    """
    attr_name = '_lazy_' + attr
    setattr(self, attr_name, value)


def unset_all_lazy(self, exclude=None):
    """
    Unset all lazy property hidden property, iow flush the cache
    """
    exclude = exclude or []
    for attr in [attr for attr in self.__dict__]:
        if attr.startswith("_lazy_") and attr[6:] not in exclude:
            try:
                delattr(self, attr)
            except AttributeError:
                pass


def unset_lazy(self, attr):
    """
    Unset <attr> lazy property hidden property, iow flush the cache
    """
    attr_name = '_lazy_' + attr
    try:
        delattr(self, attr_name)
    except AttributeError:
        pass



