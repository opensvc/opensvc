import json

from utilities.kv_store.kv_abstract import KvAbstract, NoKey
from utilities.lazy import lazy
from utilities.naming import factory, split_path
import core.exceptions as ex


class KvSec(KvAbstract):
    def __init__(self, secpath, node=None, **kwargs):
        super(KvSec, self).__init__(**kwargs)
        self.secpath = secpath
        self.node = node

    @lazy
    def sec(self):
        name, namespace, kind = split_path(self.secpath)
        return factory('sec')(name=name, namespace=namespace, node=self.node, log_handlers=["file"])

    def create(self, key, value):
        self.sec.add_key(key, json.dumps(value))

    def read(self, key):
        try:
            return json.loads(self.sec.decode_key(key))
        except ex.Error:
            raise NoKey

    def update(self, key, value):
        self.create(key, value)

    def delete(self, key):
        self.sec.remove_key(key)
