import checks
from rcUtilities import justcall

class check(checks.check):
    chk_type = "vg_u"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for rs in svc.get_res_sets('disk'):
                for r in rs.resources:
                    if not hasattr(r, "name"):
                        continue
                    if r.name == vgname:
                        return svc.svcname
        return ''

    def do_check(self):
        cmd = ['vgs', '--units', 'b', '--noheadings',
               '-o', 'vg_name,vg_size,vg_free']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        for line in lines:
            l = line.split()
            if len(l) != 3:
                continue
            size = int(l[1].replace('B',''))
            free = int(l[2].replace('B',''))
            val = int(100*(size-free)/size)
            r.append({'chk_instance': l[0],
                      'chk_value': str(val),
                      'chk_svcname': self.find_svc(l[0]),
                     }
                    )
        return r
