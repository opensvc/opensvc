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
    #print ' '.join(_cmd)
    out, err, ret = justcall(_cmd)
    if "ErrorId" in err:
        print _cmd
        print out
        raise ex.excError("dcs command execution error")
    return out, err, ret

class Dcss(object):
    arrays = []

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0
        cf = os.path.join(pathetc, "auth.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}
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
                m[manager] = [dcs, username, password]
            except:
                print "error parsing section", s
                pass
        del(conf)
        done = []
        for manager, v in m.items():
            dcs, username, password = v
            for name in dcs:
                if self.filtering and name not in self.objects:
                    continue
                if name in done:
                    continue
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
        self.keys = ['dcsservergroup',
                     'dcsserver',
                     'dcspool',
                     'dcspoolperf',
                     'dcslogicaldisk',
                     'dcslogicaldiskperf',
                     'dcsvirtualdisk',
                     'dcsphysicaldisk',
                     'dcsdiskpath',
                     'dcsport',
                     'dcspoolmember']

    def dcscmd(self, cmd):
        return dcscmd(cmd, self.manager, self.username, self.password, dcs=self.name)

    def get_dcsservergroup(self):
        cmd = 'get-dcsservergroup'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsserver(self):
        cmd = 'get-dcsserver'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspool(self):
        cmd = 'get-dcspool'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcslogicaldisk(self):
        cmd = 'get-dcslogicaldisk'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsvirtualdisk(self):
        cmd = 'get-dcsvirtualdisk'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsphysicaldisk(self):
        cmd = 'get-dcsphysicaldisk'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsdiskpath(self):
        cmd = 'get-dcsdiskpath'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspoolmember(self):
        cmd = 'get-dcspoolmember'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspoolperf(self):
        cmd = 'get-dcspool | get-dcsperformancecounter'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcslogicaldiskperf(self):
        cmd = 'get-dcslogicaldisk | get-dcsperformancecounter'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsport(self):
        cmd = 'get-dcsport'
        print "%s: %s"%(self.name, cmd)
        buff = self.dcscmd(cmd)[0]
        return buff

if __name__ == "__main__":
    o = Dcss()
    for dcs in o:
        print dcs.get_dcsserver()
