import drivers.check
from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "fs_u"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if not hasattr(resource, "mount_point"):
                    continue
                if resource.mount_point == mountpt:
                    return svc.path
        return ''

    def do_check(self):
        r = []
        for t in ['ufs', 'vxfs']:
            r += self._do_check(t)
        return r

    def _do_check(self, t):
        cmd = ['df', '-F', t, '-k']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) == 5:
                l = [''] + l
            elif len(l) != 6:
                continue
            path = self.find_svc(l[5])
            r.append({
                      "instance": l[5],
                      "value": l[4],
                      "path": path,
                     })
            r.append({
                      "instance": l[5]+".free",
                      "value": l[3],
                      "path": path,
                     })
            r.append({
                      "instance": l[5]+".size",
                      "value": l[1],
                      "path": path,
                     })
        return r
