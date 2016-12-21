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
        r = []
        for t in ['ufs', 'vxfs']:
            r += self._do_check(t)
        return r

    def _do_check(self, t):
        cmd = ['df', '-F', t, '-o', 'i']
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
            r.append({
                      'chk_instance': l[5],
                      'chk_value': l[4],
                      'chk_svcname': self.find_svc(l[5]),
                     })
        return r
