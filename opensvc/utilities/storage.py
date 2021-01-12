from copy import deepcopy
try:
    import copy_reg
except ImportError:
    import copyreg as copy_reg


class Storage(dict):
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __getitem__ = dict.get
    __getattr__ = dict.get
    __repr__ = lambda self: '<Storage %s>' % dict.__repr__(self)
    __copy__ = lambda self: Storage(self)  # pylint: disable=undefined-variable

    def __deepcopy__(self, memo=None):
        return Storage(deepcopy(dict(self), memo=memo))


def pickle_storage(s):
    return Storage, (dict(s),)


copy_reg.pickle(Storage, pickle_storage)
