import time

from utilities.kv_store.kv_abstract import KvAbstract, NoKey


class KvSimple(KvAbstract):
    def __init__(self, *args, **kwargs):
        super(KvSimple, self).__init__(*args, **kwargs)
        self.cache = {}

    def create(self, key, value):
        now = time.time()
        self.cache[key] = {
            "value": value,
            "created_at": now,
            "updated_at": now
        }

    def read(self, key):
        if key in self.cache:
            return self.cache[key].get('value', None)
        else:
            raise NoKey

    def update(self, key, value):
        now = time.time()
        self.cache[key]['value'] = value
        self.cache[key]['updated_at'] = now

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
