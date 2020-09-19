from utilities.kv_store.kv_abstract import KvAbstract, NoKey


class KvNull(KvAbstract):
    def create(self, key, value):
        pass

    def read(self, key):
        raise NoKey

    def update(self, key, value):
        raise NoKey

    def delete(self, key):
        pass
