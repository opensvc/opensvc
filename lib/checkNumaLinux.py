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
import glob
import math

class check(checks.check):
    chk_type = "numa"

    def do_check(self):
        meminfo = {}
        memtotal = 0
        n_nodes = 0
        for npath in glob.glob("/sys/devices/system/node/node*"):
            node = os.path.basename(npath)
            with open(npath+"/meminfo", 'r') as f:
                lines = f.read().strip('\n').split('\n')
                for line in lines:
                    if 'MemTotal' in line:
                        try:
                            meminfo[node] = int(line.split()[-2])
                        except:
                            continue
                        memtotal += meminfo[node]
                        n_nodes += 1
                        break
        r = []
        if n_nodes < 2:
            return r
        memavg = memtotal / n_nodes
        for node, mem in meminfo.items():
            deviation = math.fabs(100. * (mem - memavg) // memavg)
            r.append({
                  'chk_instance': node+'.mem.leveling',
                  'chk_value': str(deviation),
                  'chk_svcname': '',
                 })
        return r

