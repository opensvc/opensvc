from __future__ import print_function
from rcUtilities import justcall, which
from xml.etree.ElementTree import XML, fromstring
import rcExceptions as ex
import os
import ConfigParser

pathlib = os.path.dirname(__file__)
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))

def sssu(cmd, manager, username, password, array=None, sssubin=None):
    if sssubin is None:
        if which("sssu"):
            sssubin = "sssu"
        elif os.path.exists("/opt/opensvc/bin/sssu"):
            sssubin = "/opt/opensvc/bin/sssu"
        else:
            raise ex.excError("sssu command not found. set 'bin' in auth.conf section.")
    os.chdir(pathtmp)
    _cmd = [sssubin,
            "select manager %s username=%s password=%s"%(manager, username, password)]
    if array is not None:
        _cmd += ["select system %s"%array]
    _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    print(" ".join(_cmd))
    if "Error" in out:
        print(_cmd)
        print(out)
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
        cf = os.path.join(pathetc, "auth.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = {}
        for s in conf.sections():
            try:
                t = conf.get(s, 'type')
            except:
                continue
            if t != "eva":
                continue
            try:
                manager = conf.get(s, 'manager')
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
            except Exception as e:
                print("error parsing section", s, ":", e)
                pass
            try:
                sssubin = conf.get(s, 'bin')
            except:
                sssubin = None
            m[manager] = [username, password, sssubin]
        del(conf)
        done = []
        for manager, creds in m.items():
            username, password, sssbin = creds
            out, err = sssu('ls system', manager, username, password, sssubin=sssubin)
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
                self.arrays.append(Eva(name, manager, username, password, sssubin=sssubin))
                done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Eva(object):
    def __init__(self, name, manager, username, password, sssubin=None):
        self.name = name
        self.manager = manager
        self.username = username
        self.password = password
        self.sssubin = sssubin
        #self.keys = ['disk_group']
        self.keys = ['controller', 'disk_group', 'vdisk']

    def sssu(self, cmd):
        return sssu(cmd, self.manager, self.username, self.password, array=self.name, sssubin=self.sssubin)

    def stripxml(self, buff):
        try:
            buff = buff[buff.index("<object>"):]
        except:
            buff = ""
        lines = buff.split('\n')
        for i, line in enumerate(lines):
            if line.startswith("\\"):
                del lines[i]
        lines = ['<main>'] + lines + ['</main>']
        return '\n'.join(lines)

    def get_controller(self):
        cmd = 'ls controller full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_disk_group(self):
        cmd = 'ls disk_group full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_vdisk(self):
        cmd = 'ls vdisk full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

    def get_lun(self):
        cmd = 'ls lun full xml'
        print("%s: %s"%(self.name, cmd))
        buff = self.sssu(cmd)[0]
        return self.stripxml(buff)

if __name__ == "__main__":
    o = Evas()
    for eva in o:
        print(eva.get_controller())
