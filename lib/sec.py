import os
import sys
import base64
import re
import fnmatch
import shutil
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs
from svc import BaseSvc
from converters import print_size
from data import DataMixin
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Sec(DataMixin, BaseSvc):
    kind = "sec"
    desc = "secret"
    default_mode = 0o0600

    @lazy
    def kwdict(self):
        return __import__("secdict")

    def add_key(self, key, data):
        if not key:
            raise ex.excError("secret key name can not be empty")
        if not data:
            raise ex.excError("secret value can not be empty")
        data = "crypt:"+base64.urlsafe_b64encode(self.encrypt(data, cluster_name="join", encode=True)).decode()
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("secret key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        self.postinstall(key)

    def decode_key(self, key):
        if not key:
            raise ex.excError("secret key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.excError("secret key %s does not exist or has no value" % key)
        if data.startswith("crypt:"):
            data = data[6:]
            return self.decrypt(base64.urlsafe_b64decode(data))[1]

