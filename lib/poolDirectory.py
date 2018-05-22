from __future__ import print_function

import os

import pool
import rcExceptions as ex
from rcUtilities import lazy, justcall
from rcGlobalEnv import rcEnv

class Pool(pool.Pool):
    @lazy
    def path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def evaluated_path(self):
        try:
            return self.node.conf_get(self.section, "path")
        except ex.OptNotFound as exc:
            return exc.default

    def translate(self, section, size=None, fmt=True, mnt=None):
        fs = {
            "rtype": "fs",
            "type": "directory",
            "path": os.path.join(self.path, "{id}_"+self.section_index(section)),
        }
        bind = {
            "rtype": "fs",
            "dev": os.path.join(self.path, "{id}_"+self.section_index(section)),
        }
        if rcEnv.sysname == "Linux":
            bind["type"] = "none"
            bind["mnt_opt"] = "bind,rw"
        elif rcEnv.sysname == "SunOS":
            bind["type"] = "lofs"
        else:
            return [fs]
        if bind:
            bind["mnt"] = mnt
        else:
            bind["mnt"] = self.default_mnt
        return [fs, bind]

    def status(self):
        from converters import convert_size
        if not os.path.exists(self.evaluated_path):
            os.makedirs(self.evaluated_path)
        data = {
            "type": "directory",
        }
        cmd = ["df", "-P", self.evaluated_path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return data
        l = out.splitlines()[-1].split()
        data["free"] = l[3]
        data["used"] = l[2]
        data["size"] = l[1]
        data["head"] = self.evaluated_path
        return data

