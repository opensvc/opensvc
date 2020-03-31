from __future__ import print_function

import sys
import os
from subprocess import *

import core.exceptions as ex
from env import Env
from core.node import Node

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

def dscli(cmd, hmc1, hmc2, username, pwfile, log=None):
    if len(hmc1) != 0:
       _hmc1 = ['-hmc1', hmc1]
    else:
       _hmc1 = []
    if len(hmc2) != 0:
       _hmc2 = ['-hmc2', hmc2]
    else:
       _hmc2 = []
    _cmd = ['/opt/ibm/dscli/dscli'] + _hmc1 + _hmc2 +['-user', username, '-pwfile', pwfile]
    if log is not None:
        log.info(cmd + ' | ' + ' '.join(_cmd))
    p = Popen(_cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate(input=cmd)
    out = out.replace("dscli>", "")
    err = err.replace("dscli>", "")
    if log is not None:
        if len(out) > 0:
            log.info(out)
        if len(err) > 0:
            log.error(err)
    if p.returncode != 0:
        #print >>sys.stderr, out, err
        raise ex.Error("dscli command execution error")
    return out, err

class IbmDss(object):
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
            if stype != "ibmds":
                continue
            pwfile = os.path.join(Env.paths.pathvar, s+'.pwfile')
            if not os.path.exists(pwfile):
                raise ex.Error("%s does not exists. create it with 'dscli managepwfile ...'"%pwfile)

            try:
                username = self.node.oget(s, "username")
                hmc1 = self.node.oget(s, "hmc1")
                hmc2 = self.node.oget(s, "hmc2")
            except Exception as exc:
                print("error parsing section %s: %s" % (s, exc), file=sys.stderr)
                continue
            self.arrays.append(IbmDs(name, hmc1, hmc2, username, pwfile, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

    def get(self, array):
        for o in self.arrays:
            if o.name == array:
                return o
        raise ex.Error("%s not defined in the node/cluster configuration, or is not usable" % array)

class IbmDs(object):
    def __init__(self, name, hmc1, hmc2, username, pwfile, node=None):
        self.name = name
        self.node = node
        self.username = username
        self.pwfile = pwfile
        self.hmc1 = hmc1
        self.hmc2 = hmc2
        self.keys = ['combo']

    def dscli(self, cmd, log=None):
        return dscli(cmd, self.hmc1, self.hmc2, self.username, self.pwfile, log=log)

    def get_combo(self):
        cmd = """setenv -banner off -header on -format delim
lsextpool
lsfbvol
lsioport
lssi
lsarray
lsarraysite
lsrank"""
        print("%s: %s"%(self.name, cmd))
        return self.dscli(cmd)[0]

if __name__ == "__main__":
    o = IbmDss()
    for ibmds in o:
        print(ibmds.get_combo())
