import drivers.check

from env import Env
from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "vg_u"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for resource in svc.get_resources('disk'):
                if not hasattr(resource, "name"):
                    continue
                if resource.name == vgname:
                    return svc.path
        return ''

    def do_check(self):
        cmd = [Env.syspaths.vgs, '--units', 'b', '--noheadings',
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
            r.append({"instance": l[0],
                      "value": str(val),
                      "path": self.find_svc(l[0]),
                     }
                    )
        return r
