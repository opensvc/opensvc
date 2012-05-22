#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import checks
import os
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import glob

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

class check(checks.check):
    chk_type = "lag"
    chk_name = "Linux network link aggregate"
    bonding_p = '/proc/net/bonding'

    def do_check(self):
        l = glob.glob(self.bonding_p+'/*')
        if len(l) == 0:
            return self.undef
        r = []
        for bond in l:
            r += self.do_check_bond(bond)
        return r

    def do_check_bond(self, bond):
        r = []
        try:
            f = open(bond, 'r')
            buff = f.read()
            f.close()
        except:
            return r
        lag = os.path.basename(bond)
        inst = lag
        for line in buff.split('\n'):
            if line.startswith('Slave Interface:'):
                slave = line.split()[-1]
                inst = '.'.join((lag, slave))
            elif line.startswith('MII Status:'):
                val = line.split()[-1]
                if val == "up":
                    val = "0"
                else:
                    val = "1"
                r.append({
                          'chk_instance': inst+'.mii_status',
                          'chk_value': val,
                          'chk_svcname': '',
                         })
            elif line.startswith('Link Failure Count:'):
                val = line.split()[-1]
                r.append({
                          'chk_instance': inst+'.link_failure_count',
                          'chk_value': val,
                          'chk_svcname': '',
                         })
        return r
