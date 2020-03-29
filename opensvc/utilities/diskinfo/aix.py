import re

from utilities.proc import call, justcall
from .diskinfo import BaseDiskInfo

class DiskInfo(BaseDiskInfo):

    def __init__(self):
        self.h = {}

    def scan(self, lname):
        vid = 'unknown'
        pid = 'unknown'
        wwid = 'unknown'
        size = 'unknown'

        cmd = ['lscfg', '-vpl', lname]
        (ret, out, err) = call(cmd)

        for f in out.split('\n'):
            if "Manufacturer" in f:
                vid = f.split('.')[-1]
            if "Machine Type and Model" in f:
                pid = f.split('.')[-1]

        cmd = ['bootinfo', '-s', lname]
        out, err, ret = justcall(cmd)
        if ret == 0:
            size = int(out.strip())
        else:
            size = 0

        wwid = self.odmget(lname, 'ww_name').replace('0x', '')
        if wwid == 'unknown':
            wwid = self.get_vscsi_id(lname)

        self.h[lname] = dict(vid=vid, pid=pid, wwid=wwid, size=size)

    def get_vscsi_id(self, lname):
        cmd = ['lscfg', '-l', lname]
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'unknown'
        l = out.split()
        if len(l) < 2:
            return 'unknown'
        d = l[1]
        regex = re.compile(r'-C[0-9]+-T[0-9]+')
        d = regex.sub('', d)
        return d

    def odmget(self, lname, attr):
        cmd = ['odmget', '-q', 'name='+lname+' AND attribute='+attr, 'CuAt']
        (ret, out, err) = call(cmd)
        for f in out.split('\n'):
            if "value" not in f:
                continue
            return f.split(" = ")[-1].strip('"')
        return 'unknown'


    def devkey(self, dev):
        dev = dev.replace("/dev/", "")
        return dev

    def get(self, dev, type):
        dev = self.devkey(dev)
        if dev not in self.h:
            self.scan(dev)
        return self.h[dev][type]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

