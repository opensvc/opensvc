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
import os

class Mount(Res.Resource):
    """Define a mount resource 
    """
    def __init__(self,mountPoint=None,device=None,fsType=None,mntOpt=None,optional=False,\
                disabled=False, scsireserv=False):
        Res.Resource.__init__(self, "mount", optional, disabled)
        self.mountPoint = mountPoint
        self.device = device
        self.fsType = fsType
        self.mntOpt = mntOpt
        self.scsiReservation = scsireserv
        self.id = 'fs ' + device + '@' + mountPoint

    def set_scsireserv():
        self.scsiReservation = scsireserv

    def start(self):
        if not os.path.exists(self.mountPoint):
            try:
                os.makedirs(self.mountPoint)
            except:
                self.log.info("failed to create missing mountpoint %s" % self.mountPoint)
                raise
            self.log.info("create missing mountpoint %s" % self.mountPoint)
                
    def __str__(self):
        return "%s mnt=%s dev=%s fsType=%s mntOpt=%s" % (Res.Resource.__str__(self),\
                self.mountPoint, self.device, self.fsType, self.mntOpt)

    def __cmp__(self, other):
        """order so that deepest mountpoint can be umount first
        """
        return cmp(self.mountPoint, other.mountPoint)

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)
    print """   m=Mount("/mnt1","/dev/sda1","ext3","rw")   """
    m=Mount("/mnt1","/dev/sda1","ext3","rw")
    print "show m", m


