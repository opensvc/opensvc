import drivers.check
from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.sub_devs()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.path
        return ''

    def odmget(self, lname, attr):
        cmd = ['odmget', '-q', 'name='+lname+' AND attribute='+attr, 'CuAt']
        out, err, ret = justcall(cmd)
        for f in out.split('\n'):
            if "value" not in f:
                continue
            return f.split(" = ")[-1].strip('"')
        return None

    def disk_wwid(self, dev):
        return self.odmget(dev, 'wwid')

    def disk_id(self, dev, typ):
        if typ.startswith("vscsi"):
            return 'vscsi.'+dev
        else:
            return self.disk_wwid(dev)

    def do_check(self):
        cmd = ['lspath']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        dev = None
        wwid = None
        for line in lines:
            l = line.split()
            if len(l) != 3:
                continue
            if l[0] != 'Enabled':
                continue
            if dev is None:
                dev = l[1]
                typ = l[2]
                wwid = self.disk_id(dev, typ)
                n = 1
            elif dev is not None and wwid is not None and dev != l[1]:
                r.append({"instance": wwid,
                          "value": str(n),
                          "path": self.find_svc(dev),
                         })
                dev = l[1]
                typ = l[2]
                wwid = self.disk_id(dev, typ)
                n = 1
            else:
                n += 1
        if dev is not None and wwid is not None:
            r.append({"instance": wwid,
                      "value": str(n),
                      "path": self.find_svc(dev),
                     })
        return r
