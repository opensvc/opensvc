from rcUtilities import justcall, which
import rcExceptions as ex
import os
import ConfigParser
from rcGlobalEnv import rcEnv

if rcEnv.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.paths.pathbin

def rcmd(cmd, manager, username, key):
    _cmd = ['ssh', '-i', key, '@'.join((username, manager))]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if ret != 0:
        print(_cmd)
        print(out)
        raise ex.excError("ssh command execution error")
    return out, err

class IbmSvcs(object):
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.arrays = []
        cf = rcEnv.paths.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}
        for s in conf.sections():
            if not conf.has_option(s, "type") or \
               conf.get(s, "type") != "ibmsvc":
                continue
            if self.filtering and not s in self.objects:
                continue
            try:
                username = conf.get(s, 'username')
                key = conf.get(s, 'key')
                m[s] = [username, key]
            except:
                print("error parsing section", s)
                pass
        del(conf)
        for name, creds in m.items():
            username, key = creds
            self.arrays.append(IbmSvc(name, username, key))

    def __iter__(self):
        for array in self.arrays:
            yield(array)

class IbmSvc(object):
    def __init__(self, name, username, key):
        self.name = name
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
