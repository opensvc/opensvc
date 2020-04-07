import base64
import foreign.six as six

from utilities.lazy import lazy
from core.objects.svc import BaseSvc
from utilities.converters import print_size
from core.objects.data import DataMixin
from utilities.string import bencode, bdecode, is_string
import core.exceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Cfg(DataMixin, BaseSvc):
    kind = "cfg"
    desc = "configuration"
    default_mode = 0o0644

    @lazy
    def kwstore(self):
        from .cfgdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .cfgdict import KEYS
        return KEYS

    def _add_key(self, key, data):
        if not key:
            raise ex.Error("configuration key name can not be empty")
        if data is None:
            raise ex.Error("configuration value can not be empty")
        if not is_string(data):
            data = "base64:"+bdecode(base64.urlsafe_b64encode(data))
        elif "\n" in data:
            data = "base64:"+bdecode(base64.urlsafe_b64encode(bencode(data)))
        else:
            data = "literal:"+data
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("configuration key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        self.postinstall(key)

    def decode_key(self, key):
        if not key:
            raise ex.Error("configuration key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.Error("configuration key %s does not exist or has no value" % key)
        if data.startswith("base64:"):
            if six.PY2:
                data = str(data)
            data = data[7:]
            data = base64.urlsafe_b64decode(data)
            try:
                return data.decode()
            except:
                return data
        elif data.startswith("literal:"):
            return data[8:]
        else:
            return data

