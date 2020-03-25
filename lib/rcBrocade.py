import os
import telnetlib

import rcExceptions as ex
from node import Node
from rcGlobalEnv import rcEnv
from rcUtilities import factory
from utilities.proc import justcall, which

if rcEnv.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.paths.pathbin

def brocadetelnetcmd(cmd, switch, username, password):
    tn = telnetlib.Telnet(switch)
    tn.read_until("login: ")
    tn.write(username + '\n')
    tn.read_until("Password: ")
    tn.write(password + '\n')
    tn.read_until("> ")
    tn.write(cmd + '\n')
    tn.write('exit\n')
    out = tn.read_all()
    return out, "", 0

def brocadecmd(cmd, switch, username, key):
    _cmd = ['ssh', '-o', 'StrictHostKeyChecking=no',
                   '-o', 'ForwardX11=no',
                   '-o', 'ConnectTimeout=5',
                   '-o', 'PasswordAuthentication=no',
                   '-l', username, '-i', key, switch, cmd]
    out, err, ret = justcall(_cmd)
    if "command not found" in err:
        # bogus firmware syntax
        _cmd = ['ssh', '-o', 'StrictHostKeyChecking=no',
                       '-o', 'ForwardX11=no',
                       '-o', 'ConnectTimeout=5',
                       '-o', 'PasswordAuthentication=no',
                       '-l', username, '-i', key, switch, 'bash --login -c '+cmd]
        out, err, ret = justcall(_cmd)
    if ret != 0:
        raise ex.excError("brocade command execution error")
    return out, err, ret

class Brocades(object):
    switchs = []

    def __init__(self, objects=[], node=None):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        if node:
            self.node = node
        else:
            self.node = Node()
        done = []
        for s in self.node.conf_sections(cat="switch"):
            name = self.node.oget(s, "name")
            if not name:
                name = s.split("#", 1)[-1]
            if name in done:
                continue
            if self.filtering and name not in self.objects:
                continue
            try:
                stype = self.node.oget(s, "type")
            except:
                continue
            if stype != "brocade":
                continue
            key = self.node.oget(s, "key")
            password = self.node.oget(s, "password")
            try:
                username = self.node.oget(s, "username")
            except:
                print("no 'username' parameter in section %s" % s)
                continue
            if key is None and password is None:
                print("no 'key' nor 'password' parameter in section %s" % s)
                continue
            self.switchs.append(Brocade(name, username, key, password, node=self.node))
            done.append(name)

    def __iter__(self):
        for switch in self.switchs:
            yield(switch)

class Brocade(object):
    def __init__(self, name, username, key, password, node=None):
        self.name = name
        self.username = username
        self.password = password
        self.key = key
        self.keys = ['brocadeswitchshow', 'brocadensshow', 'brocadezoneshow']
        self.node = node

    def brocadecmd(self, cmd):
        if self.key is not None:
            return brocadecmd(cmd, self.name, self.username, self.key)
        elif self.password is not None:
            return brocadetelnetcmd(cmd, self.name, self.username, self.password)
        else:
            raise Exception("ssh nor telnet method available")

    def get_brocadeswitchshow(self):
        cmd = 'switchshow'
        print("%s: %s"%(self.name, cmd))
        buff = self.brocadecmd(cmd)[0]
        return buff

    def get_brocadensshow(self):
        cmd = 'nsshow'
        print("%s: %s"%(self.name, cmd))
        buff = self.brocadecmd(cmd)[0]
        return buff

    def get_brocadezoneshow(self):
        cmd = 'zoneshow'
        print("%s: %s"%(self.name, cmd))
        buff = self.brocadecmd(cmd)[0]
        return buff

if __name__ == "__main__":
    o = Brocades()
    for brocade in o:
        print(brocade.get_brocadeswitchshow())
