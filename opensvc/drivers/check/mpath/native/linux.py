import drivers.check

from env import Env
from utilities.proc import justcall

class Check(drivers.check.Check):
    chk_type = "mpath"
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

    def do_check_old(self):
        r"""
	mpath1 (3600508b4000971ca0000f00010650000)
	[size=404 GB][features="1 queue_if_no_path"][hwhandler="0"]
	\_ round-robin 0 [active]
	 \_ 0:0:0:2 sda 8:0   [active]
	 \_ 1:0:0:2 sde 8:64  [active]
	 \_ 1:0:1:2 sdf 8:80  [active]
	 \_ 0:0:1:2 sdi 8:128 [active]
	\_ round-robin 0 [enabled]
	 \_ 0:0:2:2 sdc 8:32  [active]
	 \_ 0:0:3:2 sdd 8:48  [active]
	 \_ 1:0:3:2 sdh 8:112 [active]
	 \_ 1:0:2:2 sdb 8:16  [active]
        """
        cmd = [Env.syspaths.multipath, '-l']
        (out, err, ret) = justcall(cmd)
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        wwid = None
        dev = None
        n = 0
        for line in lines:
            if len(line) > 0 and not r'\_ ' in line and not line.startswith('['):
                # new mpath
                # - store previous
                # - reset path counter
                if wwid is not None:
                    r.append({"instance": wwid,
                              "value": str(n),
                              "path": self.find_svc(dev),
                             })
                n = 0
                l = line.split()
                if len(l) == 2:
                    wwid = l[1][1:-1]
                elif len(l) == 1:
                    wwid = l[0]
                else:
                    wwid = None
                if wwid is not None and len(wwid) in (17, 33) and wwid[0] in ('2', '3', '5'):
                    wwid = wwid[1:]
            if "[active]" in line and line.startswith(' '):
                n += 1
                dev = "/dev/"+line.split()[2]
        if wwid is not None:
            r.append({"instance": wwid,
                      "value": str(n),
                      "path": self.find_svc(dev),
                     })
        return r

    def do_check(self):
        cmd = [Env.syspaths.multipathd, '-kshow topo']
        (out, err, ret) = justcall(cmd)
        if 'list|show' in out:
            # multipathd does not support 'show topo'
            # try parsing 'multipath -l' output
            return self.do_check_old()
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        wwid = None
        dev = None
        n = 0
        for line in lines:
            if ' dm-' in line:
                # new mpath
                # - store previous
                # - reset path counter
                if wwid is not None:
                    r.append({"instance": wwid,
                              "value": str(n),
                              "path": self.find_svc(dev),
                             })
                n = 0
                if line.startswith(": "):
                    line = line.replace(": ", "")
                l = line.split()
                if l[0].endswith(":"):
                    # skip prefix: create, swithpg, reload, ...
                    l = l[1:]
                if len(l) < 2:
                    continue
                if l[1].startswith('('):
                    wwid = l[1][1:-1]
                else:
                    wwid = l[0]
                if wwid is not None and len(wwid) in (17, 33) and wwid[0] in ('2', '3', '5'):
                    wwid = wwid[1:]
            if "[active][ready]" in line or \
               "active ready" in line:
                n += 1
                dev = "/dev/"+line.split()[2]
        if wwid is not None:
            r.append({"instance": wwid,
                      "value": str(n),
                      "path": self.find_svc(dev),
                     })
        return r
