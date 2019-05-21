import os
import sys
import base64
import re
import fnmatch
import shutil
import glob
import tempfile

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_svcpath, fmt_svcpath, factory
from svc import BaseSvc
from converters import print_size
from data import DataMixin
from rcSsl import gen_cert
from sec import Sec
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Usr(Sec, BaseSvc):
    kind = "usr"
    desc = "user"

    @lazy
    def kwdict(self):
        return __import__("usrdict")

    def on_create(self):
        if not self.oget("DEFAULT", "cn"):
            self.set_multi(["cn=%s" % self.svcname])
        if not self.oget("DEFAULT", "ca"):
            ca = self.node.oget("cluster", "ca")
            if ca is None:
                ca = "system/sec/ca-" + self.node.cluster_name
            self.set_multi(["ca=%s" % ca])
        if "certificate" not in self.data_keys():
            self.gen_cert()

