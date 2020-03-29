import os

import drivers.check

from env import Env
from utilities.proc import justcall

class Check(drivers.check.Check):
    """
    # zpool status
      pool: rpool
     state: DEGRADED
    status: One or more devices has been taken offline by the administrator.
            Sufficient replicas exist for the pool to continue functioning in a
            degraded state.
    action: Online the device using 'zpool online' or replace the device with
            'zpool replace'.
     scrub: scrub completed after 0h40m with 0 errors on Sun Jun 24 05:41:27 2012
    config:

            NAME          STATE     READ WRITE CKSUM
            rpool         DEGRADED     0     0     0
              mirror      DEGRADED     0     0     0
                c0t0d0s0  ONLINE       0     0     0
                c0t1d0s0  OFFLINE      0     0     0
    """
    chk_type = "zpool"

    def find_svc(self, pool):
        for svc in self.svcs:
            for res in svc.get_resources("disk.zpool"):
                if not hasattr(res, "name"):
                    continue
                if res.name == pool:
                    return svc.path
        return ''

    def do_check(self):
        cmd = ['zpool', 'list', '-H', '-o', 'name,health']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines:
            if len(line) < 2:
                continue
            l = line.split()
            pool = l[0]
            stat = l[1]
            if stat == 'ONLINE':
                stat_val = 0
            elif stat == 'DEGRADED':
                stat_val = 1
            elif stat == 'FAULTED':
                stat_val = 2
            elif stat == 'OFFLINE':
                stat_val = 3
            elif stat == 'REMOVED':
                stat_val = 4
            elif stat == 'UNAVAIL':
                stat_val = 5
            else:
                stat_val = 6
            if pool is not None:
                r.append({"instance": pool,
                          "value": str(stat_val),
                          "path": self.find_svc(pool),
                        })
        return r
