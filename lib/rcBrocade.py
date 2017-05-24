from rcUtilities import justcall, which
import rcExceptions as ex
import os
import ConfigParser
import telnetlib
from rcGlobalEnv import rcEnv

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

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        cf = rcEnv.paths.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = []
        for s in conf.sections():
            if self.filtering and s not in self.objects:
                continue
            try:
                stype = conf.get(s, 'type')
            except:
                continue
            if stype != "brocade":
                continue
            name = s
            key = None
            password = None
            try:
                username = conf.get(s, 'username')
            except:
                print("no 'username' parameter in %s section %s"%(cf, s))
                continue
            try:
                key = conf.get(s, 'key')
            except:
                pass
            try:
                password = conf.get(s, 'password')
            except:
                pass
            if key is None and password is None:
                print("no 'key' nor 'password' parameter in %s section %s"%(cf, s))
                continue
            m.append([name, username, key, password])
        del(conf)
        for name, username, key, password in m:
            self.switchs.append(Brocade(name, username, key, password))

    def __iter__(self):
        for switch in self.switchs:
            yield(switch)

class Brocade(object):
    def __init__(self, name, username, key, password):
        self.name = name
        self.username = username
        self.password = password
        self.key = key
        self.keys = ['brocadeswitchshow', 'brocadensshow', 'brocadezoneshow']

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
