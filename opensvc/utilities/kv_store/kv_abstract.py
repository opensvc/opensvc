class NoKey(Exception):
    pass


class KvAbstract:
    def __init__(self, name, is_expired):
        self.name = name
        self.is_expired = is_expired

    def create(self, key, value):
        raise NotImplemented

    def read(self, key):
        raise NotImplemented

    def update(self, key, value):
        raise NotImplemented

    def delete(self, key):
        raise NotImplemented

    def read_not_expired(self, key):
        try:
            data = self.read(key)
        except NoKey:
            raise
        if self.is_expired(data):
            self.delete(key)
            raise NoKey
        return data
