import drivers.check

from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "vg_u"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for resource in svc.get_resources('disk.vg'):
                if not hasattr(resource, "name"):
                    continue
                if resource.name == vgname:
                    return svc.path
        return ''

    def do_check(self):
        r = []
        cmd = ['lsvg']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        vgs = out.split('\n')
        for vg in vgs:
            r += self._do_check(vg)
        return r

    def _do_check(self, vg):
        cmd = ['lsvg', '-p', vg]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 3:
            return self.undef
        r = []
        for line in lines[2:]:
            l = line.split()
            if len(l) != 5:
                continue
            size = int(l[2])
            free = int(l[3])
            val = int(100*(size-free)/size)
            r.append({"instance": vg,
                      "value": str(val),
                      "path": self.find_svc(l[0]),
                     }
                    )
        return r
