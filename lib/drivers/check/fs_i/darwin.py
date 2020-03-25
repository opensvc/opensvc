import drivers.check

from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "fs_i"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if not hasattr(resource, "mount_point"):
                    continue
                if resource.mount_point == mountpt:
                    return svc.path
        return ''

    def do_check(self):
        cmd = ['df', '-lPi']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 9:
                continue
            if l[5].startswith('/Volumes'):
                continue
            r.append({
                      "instance": l[8],
                      "value": l[7],
                      "path": self.find_svc(l[8]),
                     })
        return r
