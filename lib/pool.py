from __future__ import print_function

import os

import rcExceptions as ex
from rcUtilities import lazy

class Pool(object):
    def __init__(self, node=None, name=None):
        self.node = node
        self.name = name

    @lazy
    def section(self):
        return "pool#"+self.name

    @lazy
    def fs_type(self):
        try:
            return self.node.conf_get(self.section, "fs_type")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mkfs_opt(self):
        try:
            return self.node.conf_get(self.section, "mkfs_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def mnt_opt(self):
        try:
            return self.node.conf_get(self.section, "mnt_opt")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def default_mnt(self):
        return os.path.join("/srv/{id}/{rindex}")

    def translate(self, size=None, fmt=True, mnt=None):
        pass

    def status(self):
        pass

