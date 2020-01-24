from copy import deepcopy


class Storage(dict):
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __getitem__ = dict.get
    __getattr__ = dict.get
    __repr__ = lambda self: '<Storage %s>' % dict.__repr__(self)
    __getstate__ = lambda self: None
    __copy__ = lambda self: Storage(self) # pylint: disable=undefined-variable

    def __deepcopy__(self, memo=None):
        return Storage(deepcopy(dict(self), memo=memo))
