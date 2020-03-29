import os
import glob
import math

import drivers.check

class Check(drivers.check.Check):
    chk_type = "numa"

    def do_check(self):
        nodeinfo = {}
        memtotal = 0
        n_nodes = 0
        n_cpu = 0
        for npath in glob.glob("/sys/devices/system/node/node*"):
            node_n_cpu = len(glob.glob(npath+"/cpu*"))
            node = os.path.basename(npath)
            with open(npath+"/meminfo", 'r') as f:
                lines = f.read().strip('\n').split('\n')
                for line in lines:
                    if 'MemTotal' in line:
                        try:
                            node_mem = int(line.split()[-2])
                        except:
                            continue
                        memtotal += node_mem
                        n_nodes += 1
                        n_cpu += node_n_cpu
                        nodeinfo[node] = {"mem": node_mem, "cpu": node_n_cpu}
                        break
        r = []
        if n_nodes < 2:
            return r
        target_per_cpu = memtotal / n_cpu
        for node, info in nodeinfo.items():
            target = target_per_cpu * info['cpu']
            deviation = math.fabs(100. * (info['mem'] - target) // target)
            r.append({
                  "instance": node+'.mem.leveling',
                  "value": str(deviation),
                  "path": '',
                 })
        return r

