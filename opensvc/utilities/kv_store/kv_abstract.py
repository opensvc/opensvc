class NoKey(Exception):
    pass


class KvAbstract(object):
    def __init__(self, name=None, is_expired=None):
        self.name = name
        self.is_expired = is_expired

    def create(self, key, value):
        raise NotImplementedError

    def read(self, key):
        raise NotImplementedError

    def update(self, key, value):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def read_not_expired(self, key):
        try:
            data = self.read(key)
        except NoKey:
            raise
        if self.is_expired and self.is_expired(data):
            self.delete(key)
            raise NoKey
        return data
