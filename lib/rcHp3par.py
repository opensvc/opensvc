import os
import json
import rcExceptions as ex
import ConfigParser
from subprocess import *
import time
import urllib
import urllib2

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

def proxy_cmd(cmd, array, manager, svcname, uuid=None, log=None):
    url = 'https://%s/api/cmd/' % manager
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
    header = { 'User-Agent' : user_agent }

    values = {
      'array' : array,
      'cmd' : cmd,
      'svcname' : svcname,
      'uuid' : uuid,
    }

    data = urllib.urlencode(values)
    req = urllib2.Request(url, data, header)

    try:
        f = urllib2.build_opener().open(req)
        response = f.read()
        #response = urllib2.urlopen(req)
    except Exception as e:
        return "", str(e)

    try:
        d = json.loads(response)
        ret = d['ret']
        out = d['out']
        err = d['err']
    except:
        ret = 1
        out = ""
        err = "unexpected proxy response format (not json)"

    if ret != 0:
        raise ex.excError("proxy error: %s" % err)

    return out, err

def cli_cmd(cmd, array, pwf, log=None):
    cmd = ['cli', '-sys', array, '-pwf', pwf, '-nohdtot', '-csvtable'] + cmd.split()
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
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

def ssh_cmd(cmd, manager, username, key, log=None):
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
    allowed_methods = ("ssh", "proxy")

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

            username = None
            manager = None
            key = None
            pwf = None

            try:
                method = conf.get(s, 'method')
            except:
                method = "ssh"

            if method not in self.allowed_methods:
                print("invalid method. allowed methods: %s" % ', '.join(self.allowed_methods))
                continue

            kwargs = {}
            try:
                manager = conf.get(s, 'manager')
                kwargs['manager'] = manager
            except:
                if method in ("proxy", "ssh"):
                    print("error parsing section", s)
                    continue

            try:
                username = conf.get(s, 'username')
                key = conf.get(s, 'key')
                kwargs['username'] = username
                kwargs['key'] = key
            except:
                if method in ("ssh"):
                    print("error parsing section", s)
                    continue

            try:
                key = conf.get(s, 'pwf')
                kwargs['pwf'] = pwf
            except:
                if method in ("cli"):
                    print("error parsing section", s)
                    continue

            self.arrays.append(Hp3par(s, method, **kwargs))

        del(conf)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Hp3par(object):
    def __init__(self, name, method, manager=None, username=None, key=None, pwf=None, svcname=""):
        self.name = name
        self.manager = manager
        self.method = method
        self.username = username
        self.pwf = pwf
        self.svcname = svcname
        self.key = key
        self.keys = ['showvv', 'showsys', 'shownode', "showcpg", "showport", "showversion"]
        self.uuid = None
        self.remotecopy = None

    def get_uuid(self):
        if self.uuid is not None:
            return self.uuid
        try:
            import ConfigParser
        except ImportError:
            import configparser as ConfigParser
        pathetc = os.path.join(os.path.dirname(__file__), '..', 'etc')
        nodeconf = os.path.join(pathetc, 'node.conf')
        config = ConfigParser.RawConfigParser()
        config.read(nodeconf)
        try:
            self.uuid = config.get("node", "uuid")
        except:
            pass
        return self.uuid

    def rcmd(self, cmd, log=None):
        if self.method == "ssh":
            return ssh_cmd(cmd, self.manager, self.username, self.key, log=log)
        elif self.method == "cli":
            return cli_cmd(cmd, self.name, self.pwf, log=log)
        elif self.method == "proxy":
            self.get_uuid()
            return proxy_cmd(cmd, self.name, self.manager, self.svcname, uuid=self.uuid, log=log)
        else:
            raise ex.excError("unsupported method %s set in auth.conf for array %s" % (self.method, self.name))

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

    def has_remotecopy(self):
        if self.remotecopy is not None:
            return self.remotecopy
        cmd = 'showlicense'
        s = self.rcmd(cmd)[0].strip("\n")
        self.remotecopy = False
        for line in s.split('\n'):
            if "Remote Copy" in line:
                self.remotecopy = True
        return self.remotecopy

    def get_showvv(self):
        if self.has_remotecopy():
            cols = ["Name", "VV_WWN", "Prov", "CopyOf", "Tot_Rsvd_MB", "VSize_MB", "UsrCPG", "CreationTime", "RcopyGroup", "RcopyStatus"]
        else:
            cols = ["Name", "VV_WWN", "Prov", "CopyOf", "Tot_Rsvd_MB", "VSize_MB", "UsrCPG", "CreationTime"]
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
