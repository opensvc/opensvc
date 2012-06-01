from rcUtilities import justcall, which
import rcExceptions as ex
import os
import ConfigParser

pathlib = os.path.dirname(__file__)
pathbin = os.path.realpath(os.path.join(pathlib, '..', 'bin'))
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))
if pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+pathbin

def brocadecmd(cmd, switch, username, key):
    _cmd = ['ssh', '-l', username, '-i', key, switch, cmd]
    out, err, ret = justcall(_cmd)
    if ret != 0:
        raise ex.excError("brocade command execution error")
    return out, err, ret

class Brocades(object):
    switchs = []

    def __init__(self):
        self.index = 0
        cf = os.path.join(pathetc, "auth.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = []
        for s in conf.sections():
            try:
                stype = conf.get(s, 'type')
            except:
                continue
            if stype != "brocade":
                continue
            try:
                name = s
                username = conf.get(s, 'username')
                key = conf.get(s, 'key')
                m.append([name, username, key])
            except:
                print "error parsing section", s
                pass
        print m
        del(conf)
        for name, username, key in m:
            self.switchs.append(Brocade(name, username, key))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.switchs):
            raise StopIteration
        self.index += 1
        return self.switchs[self.index-1]

class Brocade(object):
    def __init__(self, name, username, key):
        self.name = name
        self.username = username
        self.key = key
        self.keys = ['brocadeswitchshow']

    def brocadecmd(self, cmd):
        return brocadecmd(cmd, self.name, self.username, self.key)

    def get_brocadeswitchshow(self):
        cmd = 'switchshow'
        print "%s: %s"%(self.name, cmd)
        buff = self.brocadecmd(cmd)[0]
        return buff

if __name__ == "__main__":
    o = Brocades()
    for brocade in o:
        print brocade.get_brocadeswitchshow()
