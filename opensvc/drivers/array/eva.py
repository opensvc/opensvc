from __future__ import print_function

import os
import sys

import core.exceptions as ex
from core.node import Node
from env import Env
from utilities.naming import factory, split_path
from utilities.proc import justcall, which

def sssu(cmd, manager, username, password, array=None, sssubin=None):
    if sssubin is None:
        if which("sssu"):
            sssubin = "sssu"
        elif os.path.exists(os.path.join(Env.paths.pathbin, "sssu")):
            sssubin = os.path.join(Env.paths.pathbin, "sssu")
        else:
            raise ex.Error("sssu command not found. set 'array#%s.bin' in the node or cluster configuration." % array)
    os.chdir(Env.paths.pathtmp)
    _cmd = [sssubin,
            "select manager %s username=%s password=%s"%(manager, username, password)]
    if array is not None:
        _cmd += ["select system %s"%array]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    print(" ".join(_cmd))
    if "Error" in out:
        print(_cmd)
        print(out)
        raise ex.Error("sssu command execution error")
    return out, err

class Evas(object):
    arrays = []

    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = objects
        self.filtering = len(objects) > 0
        if node:
            self.node = node
        else:
            self.node = Node()
        done = []
        for s in self.node.conf_sections(cat="array"):
            name = s.split("#", 1)[-1]
            if name in done:
                continue
            try:
                stype = self.node.oget(s, "type")
            except:
                continue
            if stype != "eva":
                continue
            try:
                manager = self.node.oget(s, 'manager')
                username = self.node.oget(s, 'username')
                password = self.node.oget(s, 'password')
                sssubin = self.node.oget(s, 'bin')
            except Exception as exc:
                print("error parsing section %s: %s" % (s, exc), file=sys.stderr)
                pass
            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue
            out, err = sssu('ls system', manager, username, password, sssubin=sssubin)
            _in = False
            for line in out.split('\n'):
                if 'Systems avail' in line:
                    _in = True
                    continue
                if not _in:
                    continue
                name = line.strip()
                if self.filtering and name not in self.objects:
                    continue
                self.arrays.append(Eva(name, manager, username, password, sssubin=sssubin))
                done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class Eva(object):
    def __init__(self, name, manager, username, password, sssubin=None, node=None):
        self.name = name
        self.node = node
        self.manager = manager
        self.username = username
        self.password = password
        self.sssubin = sssubin
        #self.keys = ['disk_group']
        self.keys = ['controller', 'disk_group', 'vdisk']

    def sssu(self, cmd):
        return sssu(cmd, self.manager, self.username, self.password, array=self.name, sssubin=self.sssubin)

    def stripxml(self, buff):
        try:
            buff = buff[buff.index("<object>"):]
        except:
            buff = ""
        lines = buff.split('\n')
        for i, line in enumerate(lines):
            if line.startswith("\\"):
                del lines[i]
        lines = ['<main>'] + lines + ['</main>']
        return '\n'.join(lines)

    def get_controller(self):
        cmd = 'ls controller full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_disk_group(self):
        cmd = 'ls disk_group full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_vdisk(self):
        cmd = 'ls vdisk full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_lun(self):
        cmd = 'ls lun full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

if __name__ == "__main__":
    o = Evas()
    for eva in o:
        print(eva.get_controller())
