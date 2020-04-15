import drivers.check

from core.capabilities import capabilities
from env import Env
from utilities.proc import justcall
from utilities.diskinfo import DiskInfo

_di = DiskInfo()

class Check(drivers.check.Check):
    chk_type = "mpath"
    chk_name = "PowerPath"
    svcdevs = {}

    def find_svc(self, dev):
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.sub_devs()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.path
        return ''

    def do_check(self):
        """
	Pseudo name=emcpowerh
	Symmetrix ID=000290101523
	Logical device ID=17C6
	state=alive; policy=SymmOpt; priority=0; queued-IOs=0
	==============================================================================
	---------------- Host ---------------   - Stor -   -- I/O Path -  -- Stats ---
	###  HW Path                I/O Paths    Interf.   Mode    State  Q-IOs Errors
	==============================================================================
	   0 qla2xxx                   sdi       FA  9dB   active  alive      0      0
	   1 qla2xxx                   sds       FA  8dB   active  alive      0      0
        """

        if "node.x.powermt" not in capabilities:
            return self.undef

        cmd = ['powermt', 'display', 'dev=all']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return self.undef

        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef

        r = []
        dev = None
        name = None
        paths = []
        n = 0
        for line in lines:
            if len(line) == 0:
                # new mpath
                # - store previous
                # - reset path counter
                if dev is not None:
                    if len(paths) > 0:
                        did = _di.disk_id(paths[0])
                        if did is not None:
                            name = did
                    r.append({"instance": name,
                              "value": str(n),
                              "path": self.find_svc(dev),
                             })
                    paths = []
                    dev = None
                    n = 0
            if 'Pseudo name' in line:
                l = line.split('=')
                if len(l) != 2:
                    continue
                name = l[1]
                dev = "/dev/"+name
            else:
                l = line.split()
                if len(l) < 3:
                    continue
                if l[2].startswith("sd"):
                    paths.append("/dev/"+l[2])
                if "active" in line and \
                   "alive" in line:
                    n += 1
        return r
