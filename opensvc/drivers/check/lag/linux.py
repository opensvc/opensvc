import glob
import json
import os

import drivers.check
import utilities.ifconfig

from env import Env

"""
Ethernet Channel Bonding Driver: v3.4.0 (October 7, 2008)

Bonding Mode: fault-tolerance (active-backup)
Primary Slave: None
Currently Active Slave: eth0
MII Status: up
MII Polling Interval (ms): 100
Up Delay (ms): 0
Down Delay (ms): 0

Slave Interface: eth0
MII Status: up
Link Failure Count: 0
Permanent HW addr: 00:23:7d:a0:20:fa

Slave Interface: eth1
MII Status: up
Link Failure Count: 0
Permanent HW addr: 00:23:7d:a0:20:f6

"""

class Check(drivers.check.Check):
    chk_type = "lag"
    chk_name = "Linux network link aggregate"
    bonding_p = '/proc/net/bonding'

    def do_check(self):
        l = glob.glob(self.bonding_p+'/*')
        if len(l) == 0:
            return self.undef
        ifg = utilities.ifconfig.Ifconfig()
        r = []
        for bond in l:
            ifname = os.path.basename(bond)
            intf = ifg.interface(ifname)
            if intf is None:
                continue
            if len(intf.ipaddr) + len(intf.ip6addr) == 0:
                continue
            r += self.do_check_bond(bond)
        return r

    def get_cache(self, bond, slave, uptime):
        cache_p = self.cache_path(bond, slave)
        try:
            with open(cache_p, 'r') as f:
                 buff = f.read()
            data = json.loads(buff)
            prev_uptime, prev_val = data
        except:
            prev_uptime, prev_val = 0, 0

        if prev_uptime >= uptime:
            # reboot
            prev_uptime, prev_val = 0, 0

        return prev_uptime, prev_val

    def uptime(self):
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.readline().split()[0])
        return uptime_seconds

    def cache_path(self, bond, slave):
        cache_p = os.path.join(Env.paths.pathtmp, "checkLagLinux.cache."+os.path.basename(bond)+"."+slave)
        return cache_p

    def write_cache(self, bond, slave, val, uptime):
        cache_p = self.cache_path(bond, slave)
        with open(cache_p, 'w') as f: f.write(json.dumps([uptime, val]))
        try:
            with open(cache_p, 'w') as f:
                f.write(json.dumps([uptime, val]))
        except:
            pass

    def do_check_bond(self, bond):
        r = []
        try:
            f = open(bond, 'r')
            buff = f.read()
            f.close()
        except:
            return r
        n_slave = 0
        lag = os.path.basename(bond)
        inst = lag
        for line in buff.split('\n'):
            if line.startswith('Slave Interface:'):
                n_slave += 1
                slave = line.split()[-1]
                inst = '.'.join((lag, slave))
            elif line.startswith('MII Status:'):
                val = line.split()[-1]
                if val == "up":
                    val = "0"
                else:
                    val = "1"
                r.append({
                          "instance": inst+'.mii_status',
                          "value": val,
                          "path": '',
                         })
            elif line.startswith('Link Failure Count:'):
                val = int(line.split()[-1])
                uptime = self.uptime()
                prev_uptime, prev_val = self.get_cache(bond, slave, uptime)
                if uptime - prev_uptime > 3600:
                    # don't mask alerts by refreshing the cache too soon
                    self.write_cache(bond, slave, val, uptime)
                # Link Failure Count per hour
                val = 3600. * (val - prev_val) / (uptime - prev_uptime)
                r.append({
                          "instance": inst+'.link_failure_per_hour',
                          "value": "%.2f"%val,
                          "path": '',
                         })
        r.append({
                  "instance": lag+'.paths',
                  "value": str(n_slave),
                  "path": '',
                 })
        return r
