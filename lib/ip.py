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

__author__="cgaliber"
__date__ ="$10 oct. 2009 09:38:20$"

import resources as Res

class Ip(Res.Resource):
    """ basic ip resource
    """
    def __init__(self,ipDev=None,ipAdress=None,optional=False,disabled=False):
        self.ipDev=ipDev
        self.ipAdress=ipAdress
        Res.Resource.__init__(self,"ip",optional,disabled)

    def __str__(self):
        return "%s ipdev=%s adress=%s" % (Res.Resource.__str__(self),\
                                         self.ipDev, self.ipAdress)

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

    print """i1=Ip("eth0","192.168.0.173")"""
    i=Ip("eth0","192.168.0.173")
    print "show i", i
    print """i.do_action("start")"""
    i.do_action("start")

