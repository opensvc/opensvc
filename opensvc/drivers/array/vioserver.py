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
        print(' '.join(_cmd))
        print(out)
        raise ex.Error("ssh command execution error")
    return out, err

class VioServers(object):
    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = []
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
            if stype != "vioserver":
                continue
            try:
                username = self.node.oget(s, 'username')
                key = self.node.oget(s, 'key')
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            self.arrays.append(VioServer(name, username, key, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class VioServer(object):
    def __init__(self, name, username, key, node=None):
        self.name = name
        self.node = node
        self.username = username
        self.key = key
        self.keys = ['lsmap', 'bootinfo', 'lsfware', 'lsdevattr', 'lsdevvpd', 'devsize']

    def rcmd(self, cmd):
        return rcmd(cmd, self.name, self.username, self.key)

    def get_lsmap(self):
        cmd = 'ioscli lsmap -all -fmt :'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_bootinfo(self):
        cmd = 'for i in $(ioscli lsmap -all -field backing|sed "s/Backing device//"); do echo $i $(bootinfo -s $i) ; done'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsfware(self):
        cmd = 'ioscli lsfware'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsdevattr(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i $(ioscli lsdev -dev $i -attr|grep ww_name);done'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_lsdevvpd(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i ; ioscli lsdev -dev $i -vpd;done'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

    def get_devsize(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i $(bootinfo -s $i);done'
        print("%s: %s"%(self.name, cmd))
        return self.rcmd(cmd)[0]

if __name__ == "__main__":
    o = VioServers()
    for vioserver in o:
        print(vioserver.lsmap())
