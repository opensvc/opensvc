import os
import json
import rcExceptions as ex
import ConfigParser
from subprocess import *

pathlib = os.path.dirname(__file__)
pathbin = os.path.realpath(os.path.join(pathlib, '..', 'bin'))
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))
if pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+pathbin

def rcmd(cmd, manager, username, key):
    _cmd = ['ssh', '-i', key, '@'.join((username, manager))]
    cmd = 'setclienv csvtable 1 ; setclienv nohdtot 1 ; ' + cmd + ' ; exit'
    p = Popen(_cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    p.stdin.write(cmd)
    out, err = p.communicate()
    if p.returncode != 0:
        print(cmd)
        print(out)
        raise ex.excError("ssh command execution error")

    if '%' in out:
        # skip prompt
        i = out.index("%") + 2
        if i < len(out):
            out = out[i:]

    return out, err

class Hp3pars(object):
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
               conf.get(s, "type") != "hp3par":
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
            self.arrays.append(Hp3par(name, username, key))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Hp3par(object):
    def __init__(self, name, username, key):
        self.name = name
        self.username = username
        self.key = key
        self.keys = ['showvv', 'showsys', 'shownode', "showcpg", "showport"]

    def rcmd(self, cmd):
        return rcmd(cmd, self.name, self.username, self.key)

    def serialize(self, s, cols):
        l = []
        for line in s.split('\n'):
            v = line.split(',')
            h = {}
            for a, b in zip(cols, v):
                h[a] = b
            if len(h) > 1:
                l.append(h)
        return json.dumps(l)

    def get_showvv(self):
        cols = ["Name", "VV_WWN", "Prov", "CopyOf", "Tot_Rsvd_MB", "VSize_MB", "UsrCPG", "CreationTime", "RcopyGroup", "RcopyStatus"]
        cmd = 'showvv -showcols ' + ','.join(cols)
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showsys(self):
        cols = ["ID", "Name", "Model", "Serial", "Nodes", "Master", "TotalCap", "AllocCap", "FreeCap", "FailedCap"]
        cmd = 'showsys'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_shownode(self):
        cols = ["Available_Cache", "Control_Mem", "Data_Mem", "InCluster", "LED", "Master", "Name", "Node", "State"]
        cmd = 'shownode -showcols ' + ','.join(cols)
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showcpg(self):
        cols = ["Id", "Name", "Warn%", "VVs", "TPVVs", "Usr", "Snp", "Total", "Used", "Total", "Used", "Total", "Used"]
        cmd = 'showcpg'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showport(self):
        cols = ["N:S:P", "Mode", "State", "Node_WWN", "Port_WWN", "Type", "Protocol", "Label", "Partner", "FailoverState"]
        cmd = 'showport'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

if __name__ == "__main__":
    o = Hp3pars()
    for hp3par in o:
        print(hp3par.get_showvv())
        print(hp3par.get_showsys())
        print(hp3par.get_shownode())
        print(hp3par.get_showcpg())
