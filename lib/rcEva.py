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

def sssu(cmd, manager, username, password, array=None):
    os.chdir(pathtmp)
    _cmd = ['sssu',
            "select manager %s username=%s password=%s"%(manager, username, password)]
    if array is not None:
        _cmd += ["select system %s"%array]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if "Error" in out:
        print _cmd
        print out
        raise ex.excError("sssu command execution error")
    return out, err

class Evas(object):
    arrays = []

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0
        cf = os.path.join(pathetc, "sssu.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}
        for s in conf.sections():
            try:
                manager = conf.get(s, 'manager')
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m[manager] = [username, password]
            except:
                print "error parsing section", s
                pass
        del(conf)
        done = []
        for manager, creds in m.items():
            username, password = creds
            out, err = sssu('ls system', manager, username, password)
            _in = False
            for line in out.split('\n'):
                if 'Systems avail' in line:
                    _in = True
                    continue
                if not _in:
                    continue
                name = line.strip()
                if self.filtering and name not in self.objects:
                    continue
                self.arrays.append(Eva(name, manager, username, password))
                done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Eva(object):
    def __init__(self, name, manager, username, password):
        self.name = name
        self.manager = manager
        self.username = username
        self.password = password
        #self.keys = ['disk_group']
        self.keys = ['controller', 'disk_group', 'vdisk']

    def sssu(self, cmd):
        return sssu(cmd, self.manager, self.username, self.password, array=self.name)

    def stripxml(self, buff):
        buff = buff[buff.index("<object>"):]
        lines = buff.split('\n')
        for i, line in enumerate(lines):
            if line.startswith("\\"):
                del lines[i]
        lines = ['<main>'] + lines + ['</main>']
        return '\n'.join(lines)

    def get_controller(self):
        cmd = 'ls controller full xml'
        print "%s: %s"%(self.name, cmd)
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_disk_group(self):
        cmd = 'ls disk_group full xml'
        print "%s: %s"%(self.name, cmd)
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_vdisk(self):
        cmd = 'ls vdisk full xml'
        print "%s: %s"%(self.name, cmd)
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_lun(self):
        cmd = 'ls lun full xml'
        print "%s: %s"%(self.name, cmd)
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

if __name__ == "__main__":
    o = Evas()
    for eva in o:
        print eva.get_controller()
