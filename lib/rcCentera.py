from __future__ import print_function
import os
import rcExceptions as ex
import ConfigParser
from subprocess import *
from rcUtilities import which
import tempfile
from rcGlobalEnv import rcEnv

if rcEnv.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.pathbin

class Centeras(object):
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.arrays = []
        cf = rcEnv.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}

        for s in conf.sections():
            if not conf.has_option(s, "type") or \
               conf.get(s, "type") != "centera":
                continue

            if self.filtering and not s in self.objects:
                continue

            server = None
            username = None
            password = None

            kwargs = {}

            for key in ("server", "username", "password", "java_bin", "jcass_dir"):
                try:
                    kwargs[key] = conf.get(s, key)
                except:
                    print("missing parameter: %s", s)
                    continue

            self.arrays.append(Centera(s, **kwargs))

        del(conf)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class Centera(object):
    def __init__(self, name, server=None, username=None, password=None, java_bin=None, jcass_dir=None):
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
        f = tempfile.NamedTemporaryFile(prefix="centera.discover.", suffix=".xml", dir=rcEnv.pathtmp)
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

