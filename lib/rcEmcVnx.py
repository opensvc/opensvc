import os
import json
import rcExceptions as ex
import ConfigParser
from subprocess import *
from rcUtilities import justcall, which
import time
from rcGlobalEnv import rcEnv

if rcEnv.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.pathbin

def naviseccli(cmd, scope=None, spa=None, spb=None, username=None, password=None):
    if which('/opt/Navisphere/bin/naviseccli') is None:
        raise ex.excError('can not find Navicli programs in usual /opt/Navisphere/bin')

    _cmd = ['/opt/Navisphere/bin/naviseccli', '-h', spa]
    _cmd += cmd
    out, err, ret = justcall(_cmd)
    if "Security file not found" in out:
        print(_cmd)
        print(out)
        raise ex.excError("naviseccli command execution error")

    return out, err

class EmcVnxs(object):
    allowed_methods = ("secfile", "credentials")

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
               conf.get(s, "type") != "emcvnx":
                continue

            if self.filtering and not s in self.objects:
                continue

            spa = None
            spb = None
            username = None
            password = None
            scope = None

            kwargs = {}

            try:
                method = conf.get(s, 'method')
            except:
                method = "secfile"

            if method not in self.allowed_methods:
                print("invalid method. allowed methods: %s" % ', '.join(self.allowed_methods))
                continue

            try:
                spa = conf.get(s, 'spa')
                spb = conf.get(s, 'spb')
            except:
                print("error parsing section", s)
                continue

            try:
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                kwargs['username'] = username
                kwargs['password'] = password
            except:
                if method in ("credentials"):
                    print("error parsing section", s)
                    continue

            try:
                scope = conf.get(s, 'scope')
            except:
                scope = "0"

            self.arrays.append(EmcVnx(s, method, scope, spa, spb, **kwargs))

        del(conf)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class EmcVnx(object):
    def __init__(self, name, method, scope, spa, spb, username=None, password=None):
        self.name = name
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
