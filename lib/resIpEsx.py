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
# To change this template, choose Tools | Templates
# and open the template in the editor.

from rcGlobalEnv import rcEnv
resIpHv = __import__("resIp"+rcEnv.sysname)
import resIpVm

class Ip(resIpVm.Ip, resIpHv.Ip):
    def __init__(self, rid=None, ipDev=None, ipName=None,
                 mask=None, always_on=set([]), monitor=False,
                 disabled=False, tags=set([]), optional=False):
        resIpVm.Ip.__init__(self, rid=rid, ipDev=ipDev, ipName=ipName,
                            mask=mask, always_on=always_on,
                            disabled=disabled, tags=tags, optional=optional,
                            monitor=monitor)

    def check_ping(self):
        return resIpHv.Ip.check_ping(self)


if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

