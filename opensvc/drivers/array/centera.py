from __future__ import print_function

import os
import tempfile
from subprocess import *

from utilities.naming import factory, split_path
from env import Env
from core.node import Node

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

class Centeras(object):
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
            if stype != "centera":
                continue

            try:
                server = self.node.oget(s, "server")
                username = self.node.oget(s, "username")
                password = self.node.oget(s, "password")
                jcass_dir = self.node.oget(s, "jcass_dir")
                java_bin = self.node.oget(s, "java_bin")
            except:
                print("error parsing section", s, file=sys.stderr)

            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue

            self.arrays.append(Centera(name, server=server, username=username, password=password, java_bin=java_bin, jcass_dir=jcass_dir, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class Centera(object):
    def __init__(self, name, server=None, username=None, password=None, java_bin=None, jcass_dir=None, node=None):
        self.name = name
        self.server = server
        self.username = username
        self.password = password
        self.java_bin = java_bin
        self.jcass_dir = jcass_dir
        self.keys = ['discover']

    def rcmd(self, buff):
        current_ld = os.environ.get("LD_LIBRARY_PATH", "")
        if self.jcass_dir not in os.environ.get("LD_LIBRARY_PATH", ""):
            os.environ["LD_LIBRARY_PATH"] = current_ld+":"+self.jcass_dir
        cmd = [self.java_bin, "-jar", os.path.join(self.jcass_dir, "JCASScript.jar")]
        buff = "poolopen %s?name=%s,secret=%s\n" % (self.server, self.username, self.password) + buff + "\nquit"
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
        out, err = p.communicate(input=buff)
        out = out.replace(self.password, "*****")
        err = err.replace(self.password, "*****")
        print(out, err)
        return out, err

    def get_discover(self):
        f = tempfile.NamedTemporaryFile(prefix="centera.discover.", suffix=".xml", dir=Env.paths.pathtmp)
        tmp_fpath = f.name
        f.close()
        buff = "monitorDiscoverToFile %s" % tmp_fpath
        out, err = self.rcmd(buff)
        with open(tmp_fpath, "r") as f:
            s = f.read()
        os.unlink(tmp_fpath)
        return s

if __name__ == "__main__":
    o = Centeras()
    for centera in o:
        print(centera.get_discover())
        break

