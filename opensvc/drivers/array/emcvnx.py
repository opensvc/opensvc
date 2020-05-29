from __future__ import print_function

import os
from subprocess import *

import core.exceptions as ex
from core.node import Node
from env import Env
from utilities.naming import factory, split_path
from utilities.proc import justcall, which

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

def naviseccli(cmd, scope=None, spa=None, spb=None, username=None, password=None):
    if which('/opt/Navisphere/bin/naviseccli') is None:
        raise ex.Error('can not find Navicli programs in usual /opt/Navisphere/bin')

    _cmd = ['/opt/Navisphere/bin/naviseccli', '-h', spa]
    _cmd += cmd
    out, err, ret = justcall(_cmd)
    if "Security file not found" in out:
        print(_cmd)
        print(out)
        raise ex.Error("naviseccli command execution error")

    return out, err

class EmcVnxs(object):
    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
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
            if stype != "emcvnx":
                continue

            try:
                method = self.node.oget(s, "method")
                scope = self.node.oget(s, "scope")
                spa = self.node.oget(s, "spa")
                spb = self.node.oget(s, "spb")
                username = self.node.oget(s, "username")
                password = self.node.oget(s, "password")
            except Exception as exc:
                print("error parsing section %s: %s" % (s, exc), file=sys.stderr)
                continue

            if method == "credentials":
                if username is None or password is None:
                    print("error parsing section %s: username and password are mandatory" % s, file=sys.stderr)
                    continue
                try:
                    secname, namespace, _ = split_path(password)
                    password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
                except Exception as exc:
                    print("error decoding password: %s", exc, file=sys.stderr)
                    continue

            self.arrays.append(EmcVnx(name, method, scope, spa, spb, username=username, password=password, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class EmcVnx(object):
    def __init__(self, name, method, scope, spa, spb, username=None, password=None, node=None):
        self.name = name
        self.node = node
        self.spa = spa
        self.spb = spb
        self.method = method
        self.scope = scope
        self.username = username
        self.password = password
        self.keys = ['portlistsp', 'getall', 'metalunlist', 'getalllun', 'getagent', 'getarrayuid', 'storagepool', 'thinlunlistall', 'getall', 'getallrg']

    def rcmd(self, cmd, log=None):
        if self.method in 'secfile':
            return naviseccli(cmd, self.scope, self.spa, self.spb, None, None)
        else:
            return naviseccli(cmd, self.scope, self.spa, self.spb, self.username, self.password)

    def get_portlistsp(self):
        cmd = ['port', '-list']
        s = self.rcmd(cmd)[0]
        return s

    def get_getall(self):
        cmd = ['getall']
        s = self.rcmd(cmd)[0]
        return s

    def get_getallrg(self):
        cmd = ['getall', '-rg']
        s = self.rcmd(cmd)[0]
        return s

    def get_metalunlist(self):
        cmd = ['metalun', '-list']
        s = self.rcmd(cmd)[0]
        return s

    def get_getalllun(self):
        cmd = ['getall' ,'-lun']
        s = self.rcmd(cmd)[0]
        return s

    def get_getagent(self):
        cmd = ['getagent']
        s = self.rcmd(cmd)[0]
        return s

    def get_getarrayuid(self):
        cmd = ['getarrayuid']
        s = self.rcmd(cmd)[0]
        return s

    def get_storagepool(self):
        cmd = ['storagepool', '-list', '-all']
        s = self.rcmd(cmd)[0]
        return s

    def get_thinlunlistall(self):
        cmd = ['thinlun', '-list', '-all']
        s = self.rcmd(cmd)[0]
        return s

if __name__ == "__main__":
    o = EmcVnxs()
    for emcvnx in o:
        print(emcvnx.get_portlistsp())
        #print(emcvnx.get_sglist())
        #print(emcvnx.get_getalllun())
        #print(emcvnx.get_metalunlist())
