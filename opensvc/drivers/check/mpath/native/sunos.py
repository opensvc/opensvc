import drivers.check

from utilities.proc import justcall

class Check(drivers.check.Check):
    """
    # mpathadm list LU
        /dev/rdsk/c6t600507680280809AB0000000000000E7d0s2
                Total Path Count: 4
                Operational Path Count: 4
        /scsi_vhci/disk@g60050768018085d7e0000000000004e5
                Total Path Count: 1
                Operational Path Count: 1
        /dev/rdsk/c6t60050768018085D7E0000000000004E4d0s2
                Total Path Count: 1
                Operational Path Count: 1
        /dev/rdsk/c6t60050768018085D7E00000000000056Bd0s2
                Total Path Count: 4
                Operational Path Count: 4
    """
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    self.svcdevs[svc] = svc.sub_devs()
                except Exception as e:
                    self.svcdevs[svc] = []
            if dev in self.svcdevs[svc]:
                return svc.path
        return ''

    def do_check(self):
        cmd = ['mpathadm', 'list', 'LU']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 4:
            return self.undef
        r = []
        dev = None
        wwid = ""
        for line in lines:
            if "/dev/" in line:
                # new mpath
                # - remember current dev
                # - remember current wwid
                # - reset path counter
                dev = line.strip()
                wwid = line[line.index('t')+1:line.rindex('d')]
                n = 0
            elif '/disk@g' in line:
                # unmapped dev
                # - remember current dev
                # - remember current wwid
                # - reset path counter
                dev = line.strip()
                wwid = '_'+line[line.index('@g')+2:]
                n = 0
            if "Total Path Count:" in line:
                continue
            if "Operational Path Count:" in line:
                # - store current dev if valid
                # - then:
                    # - reset path counter
                    # - reset dev
                n = int(line.split(':')[-1].strip())
                if dev is not None:
                    r.append({"instance": wwid,
                              "value": str(n),
                              "path": self.find_svc(dev),
                             })
                    dev = None
                    n = 0
        return r

