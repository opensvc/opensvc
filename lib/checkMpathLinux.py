#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
from rcUtilities import justcall

class check(checks.check):
    chk_type = "mpath"

    def find_svc(self, dev):
        for svc in self.svcs:
            if dev in svc.disklist():
                return svc.svcname
        return ''

    def do_check_old(self):
        """
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
        cmd = ['multipath', '-l']
        (out, err, ret) = justcall(cmd)
        lines = out.split('\n')
        if len(lines) < 1:
            return self.undef
        r = []
        wwid = None
        dev = None
        for line in lines:
            if len(line) > 0 and not '\_ ' in line and not line.startswith('['):
                # new mpath
                # - store previous
                # - reset path counter
                if wwid is not None:
                    r.append({'chk_instance': wwid,
                              'chk_value': str(n),
                              'chk_svcname': self.find_svc(dev),
                             })
                n = 0
                l = line.split()
                if len(l) == 2:
                    wwid = l[1][2:-1]
                elif len(l) == 1:
                    wwid = l[0][1:]
                else:
                    wwid = None
            if "[active]" in line and line.startswith(' '):
                n += 1
                dev = "/dev/"+line.split()[2]
        if wwid is not None:
            r.append({'chk_instance': wwid,
                      'chk_value': str(n),
                      'chk_svcname': self.find_svc(dev),
                     })
        return r

    def do_check(self):
        cmd = ['multipathd', '-kshow topo']
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
        for line in lines:
            if ' dm-' in line:
                # new mpath
                # - store previous
                # - reset path counter
                if wwid is not None:
                    r.append({'chk_instance': wwid,
                              'chk_value': str(n),
                              'chk_svcname': self.find_svc(dev),
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
                    wwid = l[1][2:-1]
                else:
                    wwid = l[0][1:]
            if "[active][ready]" in line or \
               "active ready" in line:
                n += 1
                dev = "/dev/"+line.split()[2]
        if wwid is not None:
            r.append({'chk_instance': wwid,
                      'chk_value': str(n),
                      'chk_svcname': self.find_svc(dev),
                     })
        return r
