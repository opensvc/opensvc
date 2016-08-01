import checks
from rcUtilities import justcall

class check(checks.check):
    chk_type = "fs_i"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for rs in svc.get_res_sets('fs'):
                for r in rs.resources:
                    if r.mountPoint == mountpt:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['df', '-i']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 7:
                continue
            if l[1] == '-':
                continue
            if ":/" in l[0]:
                continue
            r.append({
                      'chk_instance': l[6],
                      'chk_value': l[5],
                      'chk_svcname': self.find_svc(l[6]),
                     })
        return r
