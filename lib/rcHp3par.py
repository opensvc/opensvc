import os
import json
import rcExceptions as ex
import ConfigParser
from subprocess import *
import time

pathlib = os.path.dirname(__file__)
pathbin = os.path.realpath(os.path.join(pathlib, '..', 'bin'))
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))
if pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+pathbin

def reformat(s):
    lines = s.split('\n')
    for i, line in enumerate(lines):
        if '%' in line:
            # skip prompt
            x = line.index("%") + 2
            if x < len(line):
                line = line[x:]
            elif x == len(line):
                line = ""
        lines[i] = line
    s = '\n'.join(lines)
    s = s.replace("Pseudo-terminal will not be allocated because stdin is not a terminal.", "")
    return s.strip()

def rcmd(cmd, manager, username, key, log=None):
    _cmd = ['ssh', '-i', key, '@'.join((username, manager))]
    cmd = 'setclienv csvtable 1 ; setclienv nohdtot 1 ; ' + cmd + ' ; exit'
    return _rcmd(_cmd, cmd, log=log)

def _rcmd(_cmd, cmd, log=None, retry=10):
    p = Popen(_cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    p.stdin.write(cmd)
    out, err = p.communicate()
    out = reformat(out)
    err = reformat(err)

    if p.returncode != 0:
        if ("Connection closed by remote host" in err or "Too many local CLI connections." in err) and retry > 0:
            if log is not None:
                log.info("3par connection refused. try #%d" % retry)
            time.sleep(1)
            return _rcmd(_cmd, cmd, log=log, retry=retry-1)
        if log is not None:
            if len(out) > 0: log.info(out)
            if len(err) > 0: log.error(err)
        else:
            print(cmd)
            print(out)
        raise ex.excError("3par command execution error")

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
        self.keys = ['showvv', 'showsys', 'shownode', "showcpg", "showport", "showversion"]

    def rcmd(self, cmd, log=None):
        return rcmd(cmd, self.name, self.username, self.key, log=log)

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

    def get_showversion(self):
        cmd = 'showversion -s'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0].strip("\n")
        return json.dumps({"Version": s})

if __name__ == "__main__":
    o = Hp3pars()
    for hp3par in o:
        print(hp3par.get_showvv())
        print(hp3par.get_showsys())
        print(hp3par.get_shownode())
        print(hp3par.get_showcpg())
