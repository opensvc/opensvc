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
        cmd = ['df', '-P']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 6:
                continue
            if l[1] == '-':
                continue
            if ":/" in l[0]:
                continue
            r.append({
                      "instance": l[5],
                      "value": l[4],
                      "path": self.find_svc(l[5]),
                     })
        return r
