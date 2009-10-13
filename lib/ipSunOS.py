#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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
"Module implement SunOS specific mounts"

__author__="cgaliber"
__date__ ="$11 oct. 2009 14:38:00$"

import ip

class Ip(ip.Ip):
    """ define ip SunOS start/stop doAction """
    def do_action(self,action=None):
        if action == "start" :
            print "====== exec ifconfig %s plumb" % (self.ipDev)
        elif action == "stop" :
            print "====== exec ifconfig %s unplumb" % (self.ipDev)
        else :
            ip.Ip.do_action(self,action)

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

