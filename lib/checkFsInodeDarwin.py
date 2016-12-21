import checks
from rcUtilities import justcall

class check(checks.check):
    chk_type = "fs_i"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if resource.mount_point == mountpt:
                    return svc.svcname
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
                      'chk_instance': l[8],
                      'chk_value': l[7],
                      'chk_svcname': self.find_svc(l[8]),
                     })
        return r
