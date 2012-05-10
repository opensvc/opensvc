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
# To change this template, choose Tools | Templates
# and open the template in the editor.

from rcGlobalEnv import rcEnv
Res = __import__("resMountHP-UX")

class Mount(Res.Mount):
    def __init__(self, rid, mountPoint, device, fsType, mntOpt,
                 snap_size=None, always_on=set([]),
                 disabled=False, tags=set([]), optional=False,
                 monitor=False):
        self.sgname = device
        Res.Mount.__init__(self, rid, mountPoint, device, fsType, mntOpt,
                           snap_size, always_on,
                           disabled=disabled, tags=tags, optional=optional,
                           monitor=monitor)

    def is_up(self):
        if self.mon_name in self.svc.cmviewcl['resource']:
            state = self.svc.cmviewcl['resource'][self.mon_name][('status', rcEnv.nodename)]
            if state == "up":
                return True
            else:
                return False
        else:
            return Res.Mount.is_up(self)

    def start(self):
        pass

    def stop(self):
        pass

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

