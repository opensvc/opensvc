from rcUtilities import justcall, which
from xml.etree.ElementTree import XML, fromstring
import rcExceptions as ex
import os
import ConfigParser

pathlib = os.path.dirname(__file__)
pathbin = os.path.realpath(os.path.join(pathlib, '..', 'bin'))
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))
if pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+pathbin

def dcscmd(cmd, manager, username, password, dcs=None):
    _cmd = ['ssh', manager]
    if dcs is not None:
        _cmd += ["connect-dcsserver -server %s -username %s -password %s ; "%(dcs, username, password)+\
                 cmd+ " ; disconnect-dcsserver"]
    else:
        _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if "ErrorId" in err:
        print _cmd
        print out
        raise ex.excError("sssu command execution error")
    return out, err, ret

class Dcss(object):
    arrays = []

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
            if stype != "datacore":
                continue
            try:
                manager = s
                dcs = conf.get(s, 'dcs').split()
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m += [(manager, dcs, username, password)]
            except:
                print "error parsing section", s
                pass
        del(conf)
        done = []
        for manager, dcs, username, password in m:
            for name in dcs:
                self.arrays.append(Dcs(name, manager, username, password))
                done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Dcs(object):
    def __init__(self, name, manager, username, password):
        self.name = name
        self.manager = manager
        self.username = username
        self.password = password
        #self.keys = ['disk_group']
        self.keys = ['dcsserver', 'dcspool', 'dcslogicaldisk', 'dcsvirtualdisk']

    def dcscmd(self, cmd):
        return dcscmd(cmd, self.manager, self.username, self.password, dcs=self.name)

    def stripxml(self, buff):
        return buff

    def get_dcsserver(self):
        cmd = 'get-dcsserver -server %s'%self.name
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return self.stripxml(buff)

    def get_dcspool(self):
        cmd = 'get-dcspool -server %s'%self.name
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return self.stripxml(buff)

    def get_dcslogicaldisk(self):
        cmd = 'get-dcslogicaldisk -server %s'%self.name
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return self.stripxml(buff)

    def get_dcsvirtualdisk(self):
        cmd = 'get-dcsvirtualdisk -server %s'%self.name
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return self.stripxml(buff)

if __name__ == "__main__":
    o = Dcss()
    for dcs in o:
        print dcs.get_dcsserver()
