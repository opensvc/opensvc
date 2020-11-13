import os
import telnetlib

import core.exceptions as ex
from core.node import Node
from env import Env
from utilities.naming import split_path, factory
from utilities.proc import justcall

if Env.paths.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+Env.paths.pathbin

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

def brocadecmd(cmd, switch, username, key=None, password=None):
    base = ['ssh', '-o', 'StrictHostKeyChecking=no',
                   '-o', 'ForwardX11=no',
                   '-o', 'ConnectTimeout=5']
    if password:
        _cmd = ["sshpass", "-d", "0"] + base + [
            '-l', username, switch
        ]
    else:
        _cmd = base + [
            '-o', 'PasswordAuthentication=no',
            '-l', username, '-i', key, switch
        ]
    out, err, ret = justcall(_cmd + [cmd], input=password)
    if "command not found" in err:
        # bogus firmware syntax
        out, err, ret = justcall(_cmd + ['bash --login -c '+cmd])
    if ret != 0:
        raise ex.Error("brocade command execution error: %s" % err)
    return out, err, ret

class Brocades(object):
    switchs = []

    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
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
            method = self.node.oget(s, "method")
            password = self.node.oget(s, "password")
            if password and "sec/" in password:
                _name, _namespace, _ = split_path(password)
                sec = factory("sec")(name=_name, namespace=_namespace, volatile=True)
                if not sec.exists():
                    print("%s referenced in section %s does not exists" % (password, s))
                    continue
                if not sec.has_key("password"):
                    print("%s referenced in section %s has no 'password' key" % (password, s))
                    continue
                password = sec.decode_key("password")
            try:
                username = self.node.oget(s, "username")
            except:
                print("no 'username' parameter in section %s" % s)
                continue
            if key is None and password is None:
                print("no 'key' nor 'password' parameter in section %s" % s)
                continue
            self.switchs.append(Brocade(name, username, key, password, method=method, node=self.node))
            done.append(name)

    def __iter__(self):
        for switch in self.switchs:
            yield(switch)

class Brocade(object):
    def __init__(self, name, username, key, password, method=None, node=None):
        self.name = name
        self.username = username
        self.password = password
        self.method = method
        self.key = key
        self.keys = ['brocadeswitchshow', 'brocadensshow', 'brocadezoneshow']
        self.node = node

    def brocadecmd(self, cmd):
        if self.key is not None or self.method == "ssh":
            return brocadecmd(cmd, self.name, self.username, key=self.key, password=self.password)
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
