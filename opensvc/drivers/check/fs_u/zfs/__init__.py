import re

import drivers.check

from utilities.converters import convert_size
from env import Env
from utilities.proc import justcall, which

class Check(drivers.check.Check):
    def __init__(self, svcs=None):
        if svcs is None:
            svcs = []
        super(Check, self).__init__(svcs)
        self.zpcache = {}

    chk_type = "fs_u"

    def get_zonepath(self, name):
        if name in self.zpcache:
            return self.zpcache[name]
        if which("zonecfg") is None:
            return
        cmd = ['zonecfg', '-z', name, 'info', 'zonepath']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return None
        self.zpcache[name] = out.split()[-1].strip()
        return self.zpcache[name]

    def find_svc(self, name, mnt):
        for svc in self.svcs:
            for resource in svc.get_resources('container.zone'):
                zpath = self.get_zonepath(resource.name)
                if zpath is not None and zpath == mnt:
                    return svc.path
            for resource in svc.get_resources('fs'):
                if hasattr(resource, "device") and resource.device == name:
                    return svc.path
        return ''

    def do_check(self):
        cmd = [Env.syspaths.zfs, 'list', '-o', 'name,used,avail,mountpoint', '-H']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        for line in lines:
            l = line.split()
            if len(l) != 4:
                continue
            if "@" in l[0]:
                # do not report clone usage
                continue
            if re.findall("/[0-9a-f]{64}", l[0]):
                # container id
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            used = convert_size(l[1], _to="KB")
            avail = convert_size(l[2], _to="KB")
            total = used + avail
            pct = round(used / total * 100)
            path = self.find_svc(l[0], l[3])
            r.append({
                      "instance": l[0],
                      "value": str(pct)+"%",
                      "path": path,
                     })
            r.append({
                      "instance": l[0]+".free",
                      "value": str(avail),
                      "path": path,
                     })
            r.append({
                      "instance": l[0]+".size",
                      "value": str(total),
                      "path": path,
                     })
        return r

