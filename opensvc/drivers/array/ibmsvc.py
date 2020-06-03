from __future__ import print_function

import os
import sys

import core.exceptions as ex

from env import Env
from core.node import Node
from utilities.proc import justcall

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

def rcmd(cmd, manager, username, key):
    _cmd = ['ssh', '-i', key, '@'.join((username, manager))]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if ret != 0:
        print(_cmd)
        print(out)
        raise ex.Error("ssh command execution error")
    return out, err

class IbmSvcs(object):
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
            if stype != "ibmsvc":
                continue

            try:
                username = self.node.oget(s, 'username')
                key = self.node.oget(s, 'key')
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            self.arrays.append(IbmSvc(name, username, key, node=self.node))

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class IbmSvc(object):
    def __init__(self, name, username, key, node=None):
        self.name = name
        self.node = node
        self.username = username
        self.key = key
        #self.keys = ['lsvdisk']
        self.keys = ['lsvdisk', 'lsmdiskgrp', 'lsnode', 'lscluster', 'svc_product_id', 'lsfabric']

    def rcmd(self, cmd):
        return rcmd(cmd, self.name, self.username, self.key)

    def get_lsvdisk(self):
        cmd = 'lsvdisk -delim :'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsmdiskgrp(self):
        cmd = 'lsmdiskgrp -delim :'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsnode(self):
        cmd = 'svcinfo lsnode -delim !'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lscluster(self):
        cmd = 'svcinfo lscluster -delim :'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsfabric(self):
        cmd = 'lsfabric -delim :'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_svc_product_id(self):
        cmd = 'echo $SVC_PRODUCT_ID'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

if __name__ == "__main__":
    o = IbmSvcs()
    for ibmsvc in o:
        print(ibmsvc.lsmdiskgrp())
