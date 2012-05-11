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

Res = __import__("resIpHP-UX")
from rcGlobalEnv import rcEnv
import rcStatus

class Ip(Res.Ip):
    def get_cntl_subnet(self, ip):
        for i in self.svc.cntl['ip']:
            data = self.svc.cntl['ip'][i]
            if data['IP'] == ip:
                return data['SUBNET']
        return None

    def _status(self, verbose=False):
        subnet = self.get_cntl_subnet(self.ipName)
        if subnet is None:
            return Res.Ip._status(self, verbose)
        if 'subnet' in self.svc.cmviewcl and \
           subnet in self.svc.cmviewcl['subnet'] and \
           ('status', rcEnv.nodename) in self.svc.cmviewcl['subnet'][subnet]:
            state = self.svc.cmviewcl['subnet'][subnet][('status', rcEnv.nodename)]
            if state == "up":
                return rcStatus.UP
            else:
                return rcStatus.DOWN
        else:
            return Res.Ip._status(self, verbose)

    def start(self):
        return 0

    def stop(self):
        return 0

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

