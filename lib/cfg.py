import os
import sys
import base64
import re
import fnmatch
import shutil
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, is_string
from svc import BaseSvc
from converters import print_size
from data import DataMixin
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Cfg(DataMixin, BaseSvc):
    kind = "cfg"
    desc = "configuration"
    default_mode = 0o0644

    @lazy
    def kwdict(self):
        return __import__("cfgdict")

    def _add_key(self, key, data):
        if not key:
            raise ex.excError("configuration key name can not be empty")
        if not data:
            raise ex.excError("configuration value can not be empty")
        if not is_string(data) or "\n" in data:
            data = "base64:"+base64.urlsafe_b64encode(data.encode()).decode()
        else:
            data = "literal:"+data
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("configuration key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        self.postinstall(key)

    def decode_key(self, key):
        if not key:
            raise ex.excError("configuration key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.excError("configuration key %s does not exist or has no value" % key)
        if data.startswith("base64:"):
            data = data[7:]
            return base64.urlsafe_b64decode(data).decode()
        elif data.startswith("literal:"):
            return data[8:]
        else:
            return data

