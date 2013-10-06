import rcExceptions as ex
import os
import ConfigParser
import tempfile
from subprocess import *

pathlib = os.path.dirname(__file__)
pathbin = os.path.realpath(os.path.join(pathlib, '..', 'bin'))
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))
pathvar = os.path.realpath(os.path.join(pathlib, '..', 'var'))
if pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+pathbin

def dscli(cmd, hmc1, hmc2, username, pwfile):
    _cmd = ['/opt/ibm/dscli/dscli', '-hmc1', hmc1, '-hmc2', hmc2, '-user', username, '-pwfile', pwfile]
    p = Popen(_cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate(input=cmd)
    if p.returncode != 0:
        print >>sys.stderr, out, err
        raise ex.excError("dscli command execution error")
    return out, err

class IbmDss(object):
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.arrays = []
        self.index = 0
        cf = os.path.join(pathetc, "auth.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}
        for s in conf.sections():
            if not conf.has_option(s, "type") or \
               conf.get(s, "type") != "ibmds":
                continue
            if self.filtering and not s in self.objects:
                continue
            pwfile = os.path.join(pathvar, s+'.pwfile')
            if not os.path.exists(pwfile):
                print >>sys.stderr, pwfile, "does not exists. create it with 'dscli managepwfile ...'"
                continue

            try:
                username = conf.get(s, 'username')
                hmc1 = conf.get(s, 'hmc1')
                hmc2 = conf.get(s, 'hmc2')
                m[s] = [hmc1, hmc2, username, pwfile]
            except Exception as e:
                print("error parsing section", s, ":", e)
                pass
        del(conf)
        for name, creds in m.items():
            hmc1, hmc2, username, pwfile = creds
            self.arrays.append(IbmDs(name, hmc1, hmc2, username, pwfile))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class IbmDs(object):
    def __init__(self, name, hmc1, hmc2, username, pwfile):
        self.name = name
        self.username = username
        self.pwfile = pwfile
        self.hmc1 = hmc1
        self.hmc2 = hmc2
        self.keys = ['combo']

    def dscli(self, cmd):
        return dscli(cmd, self.hmc1, self.hmc2, self.username, self.pwfile)

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
