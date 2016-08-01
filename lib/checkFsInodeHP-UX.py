import checks
from rcUtilities import call

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
        """
        # bdf -li
        Filesystem          kbytes    used   avail %used  iused  ifree %iuse Mounted on
        /dev/vg00/lvol3    1048576  228160  814136   22%   2105  25607    8% /
        """
        cmd = ['bdf', '-li']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) == 9:
                inst = ' '.join(l[8:])
                r.append({'chk_instance': inst,
                      'chk_value': l[7],
                      'chk_svcname': self.find_svc(inst),
                     }
                    )
            elif len(l) == 8:
                inst = ' '.join(l[7:])
                r.append({'chk_instance': inst,
                      'chk_value': l[6],
                      'chk_svcname': self.find_svc(inst),
                     }
                    )
        return r
