import rcExceptions as ex
import os
import ConfigParser
import tempfile
import sys
from subprocess import *
from rcGlobalEnv import rcEnv

if rcEnv.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.pathbin

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
        cf = rcEnv.authconf
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
            pwfile = os.path.join(rcEnv.pathvar, s+'.pwfile')
            if not os.path.exists(pwfile):
                raise ex.excError("%s does not exists. create it with 'dscli managepwfile ...'"%pwfile)

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

    def get(self, array):
        for o in self.arrays:
            if o.name == array:
                return o
        raise ex.excError("%s not defined in auth.conf or not usable" % array)

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
