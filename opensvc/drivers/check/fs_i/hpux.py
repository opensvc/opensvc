import drivers.check
from utilities.proc import call

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
                r.append({"instance": inst,
                      "value": l[7],
                      "path": self.find_svc(inst),
                     }
                    )
            elif len(l) == 8:
                inst = ' '.join(l[7:])
                r.append({"instance": inst,
                      "value": l[6],
                      "path": self.find_svc(inst),
                     }
                    )
        return r
