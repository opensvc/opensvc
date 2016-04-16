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

Res = __import__("resIpLinux")

import rcStatus
from rcUtilities import justcall

class Ip(Res.Ip):

    def start(self):
        return 0

    def stop(self):
        return 0

    def _status(self, verbose=False):
        cmd = ['ip', 'addr', 'ls']
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.status_log("%s exec failed"%' '.join(cmd))
            return rcStatus.WARN
        if " "+self.addr+"/" in out:
            return rcStatus.UP
        return rcStatus.DOWN

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

