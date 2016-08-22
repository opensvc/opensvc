import checks
from rcUtilities import justcall, which

class check(checks.check):
    def __init__(self, svcs=[]):
        checks.check.__init__(self, svcs)
        self.zpcache = {}

    chk_type = "fs_u"

    def convert(self, s):
        s = s.replace(',', '.').upper()
        if s == "0":
            return 0
        if len(s) < 2:
            raise
        if s.endswith('T'):
            s = float(s[:-1])*1024*1024*1024
        elif s.endswith('G'):
            s = float(s[:-1])*1024*1024
        elif s.endswith('M'):
            s = float(s[:-1])*1024
        elif s.endswith('K'):
            s = float(s[:-1])
        else:
            raise
        return s

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
            for rs in svc.get_res_sets('container'):
                for r in rs.resources:
                    if  r.type == "container.zone":
                        zp = self.get_zonepath(r.name)
                        if zp is not None and zp == mnt:
                            return svc.svcname
            for rs in svc.get_res_sets('fs'):
                for r in rs.resources:
                    if hasattr(r, "device") and r.device == name:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['zfs', 'list', '-o', 'name,used,avail,mountpoint', '-H']
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
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            used = self.convert(l[1])
            avail = self.convert(l[2])
            total = used + avail
            pct = used / total * 100
            r.append({
                      'chk_instance': l[0],
                      'chk_value': str(pct),
                      'chk_svcname': self.find_svc(l[0], l[3]),
                     })
        return r
