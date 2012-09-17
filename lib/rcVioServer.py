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

def rcmd(cmd, manager, username, key):
    _cmd = ['ssh', '-i', key, '@'.join((username, manager))]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if ret != 0:
        print ' '.join(_cmd)
        print out
        raise ex.excError("ssh command execution error")
    return out, err

class VioServers(object):
    def __init__(self, objects=[]):
        self.objects = []
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
            if self.filtering and s not in self.objects:
                continue
            if not conf.has_option(s, "type") or \
               conf.get(s, "type") != "vioserver":
                continue
            try:
                username = conf.get(s, 'username')
                key = conf.get(s, 'key')
                m[s] = [username, key]
            except:
                print "error parsing section", s
                pass
        del(conf)
        for name, creds in m.items():
            username, key = creds
            self.arrays.append(VioServer(name, username, key))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class VioServer(object):
    def __init__(self, name, username, key):
        self.name = name
        self.username = username
        self.key = key
        self.keys = ['lsmap', 'bootinfo', 'lsfware', 'lsdevattr', 'lsdevvpd', 'devsize']

    def rcmd(self, cmd):
        return rcmd(cmd, self.name, self.username, self.key)

    def get_lsmap(self):
        cmd = 'ioscli lsmap -all -fmt :'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

    def get_bootinfo(self):
        cmd = 'for i in $(ioscli lsmap -all -field backing|sed "s/Backing device//"); do echo $i $(bootinfo -s $i) ; done'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

    def get_lsfware(self):
        cmd = 'ioscli lsfware'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

    def get_lsdevattr(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i $(ioscli lsdev -dev $i -attr|grep ww_name);done'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

    def get_lsdevvpd(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i ; ioscli lsdev -dev $i -vpd;done'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

    def get_devsize(self):
        cmd = 'for i in $(ioscli lsdev -type disk -field name -fmt .) ; do echo $i $(bootinfo -s $i);done'
        print "%s: %s"%(self.name, cmd)
        return self.rcmd(cmd)[0]

if __name__ == "__main__":
    o = VioServers()
    for vioserver in o:
        print vioserver.lsmap()
