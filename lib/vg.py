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

import resources as Res
from rcGlobalEnv import rcEnv
import scsiReserv as scsiReserv

class Vg(Res.Resource):
    """ basic vg resource
    """
    def __init__(self, vgName=None, optional=False, disabled=False, scsireserv=False):
        Res.Resource.__init__(self, "vg", optional, disabled)
        self.vgName = vgName
        self.scsiReservation = scsireserv
        self.id = 'vg ' + vgName

    def set_scsireserv(self):
        self.scsiReservation = True

    def scsirelease(self):
        return scsiReserv.ScsiReserv(self.disklist()).scsirelease()

    def scsireserv(self):
        return scsiReserv.ScsiReserv(self.disklist()).scsireserv()

    def scsicheckreserv(self):
        return scsiReserv.ScsiReserv(self.disklist()).scsicheckreserv()

    def disklist(self):
        return []

    def vgstop(self):
        pass

    def vgstart(self):
        pass

    def stop(self):
        if self.scsirelease() != 0:
            return 1
        if self.vgstop() != 0:
            return 1
        return 0

    def start(self):
        if self.vgstart() != 0:
            return 1
        if self.scsireserv() != 0:
            return 1
        return 0

    def __str__(self):
        return "%s vgname=%s" % (Res.Resource.__str__(self),\
                                 self.vgName)

if __name__ == "__main__":
    for c in (Vg,) :
        help(c)

    print """v1=vg("myvg")"""
    v=vg("myvg")
    print "show v", v
    print """v.do_action("start")"""
    v.do_action("start")

