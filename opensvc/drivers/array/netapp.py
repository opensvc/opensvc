from __future__ import print_function

import os
from subprocess import *

from env import Env
from utilities.naming import factory, split_path
from core.node import Node
from utilities.proc import justcall

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

class Netapps(object):
    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = objects
        self.filtering = len(objects) > 0
        self.arrays = []
        if node:
            self.node = node
        else:
            self.node = Node()
        done = []
        for s in self.node.conf_sections(cat="array"):
            name = s.split("#", 1)[-1]
            if name in done:
                continue
            if self.filtering and name not in self.objects:
                continue
            try:
                stype = self.node.oget(s, "type")
            except:
                continue
            if stype != "netapp":
                continue

            kwargs = {"node": self.node}

            for key in ("server", "username", "key"):
                try:
                    kwargs[key] = self.node.oget(s, key)
                except:
                    print("missing parameter: %s", s)
            if "server" not in kwargs or "username" not in kwargs or "key" not in kwargs:
                continue
            try:
                secname, namespace, _ = split_path(kwargs["password"])
                kwargs["password"] = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue
            self.arrays.append(Netapp(s, **kwargs))

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class Netapp(object):
    def __init__(self, name, server=None, username=None, key=None, node=None):
        self.name = name
        self.server = server
        self.username = username
        self.key = key
        self.keys = [
          'aggr_show_space',
          'lun_show_v',
          'lun_show_m',
          'sysconfig_a',
          'df',
          'df_S',
          'fcp_show_adapter',
        ]

    def rcmd(self, cmd):
        cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-i", self.key, self.username+"@"+self.server, cmd]
        out, err, ret = justcall(cmd)
        return out, err

    def get_aggr_show_space(self):
        out, err = self.rcmd("aggr show_space -m")
        return out

    def get_lun_show_v(self):
        out, err = self.rcmd("lun show -v")
        return out

    def get_lun_show_m(self):
        out, err = self.rcmd("lun show -m")
        return out

    def get_sysconfig_a(self):
        out, err = self.rcmd("sysconfig -a")
        return out

    def get_df(self):
        out, err = self.rcmd("df")
        return out

    def get_df_S(self):
        out, err = self.rcmd("df -S")
        return out

    def get_fcp_show_adapter(self):
        out, err = self.rcmd("fcp show adapter")
        return out

if __name__ == "__main__":
    o = Netapps()
    for netapp in o:
        print(netapp.get_aggr_show_space())
        break

