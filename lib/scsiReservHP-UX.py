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
import uuid
import re
import time
import rcStatus
import rcExceptions as ex
from rcUtilities import which
from subprocess import *
import scsiReserv

def scsireserv_supported():
    if which('scu') is None:
        return False
    return True

def rdisk(disk):
    d = disk.replace("/dev/disk", "/dev/rdisk")
    d = d.replace("/dev/dsk", "/dev/rdsk")
    return d

class ScsiReserv(scsiReserv.ScsiReserv):
    def __init__(self, disks):
        scsiReserv.ScsiReserv.__init__(self, disks)
        self.prtype = 'wero'

    def ack_unit_attention(self, d):
        return 0

    def disk_registered(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'show', 'keys' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_register(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'preserve', 'register', 'skey', self.hostid ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'preserve', 'clear', 'key', self.hostid ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

    def get_reservation_key(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'show', 'reservation' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to list reservation for disk %s" % disk)
        if 'Reservation Key' not in out:
            return None
        for line in out.split('\n'):
            if 'Reservation Key' in line:
                return line.split()[-1]
        raise Exception()

    def disk_reserved(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'show', 'reservation' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read reservation for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_release(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'preserve', 'release', 'key', self.hostid, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def disk_reserve(self, disk):
        cmd = [ 'scu', '-f', rdisk(disk), 'preserve', 'reserve', 'key', self.hostid, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def disk_preempt_reservation(self, disk, oldkey):
        cmd = [ 'scu', '-f', rdisk(disk), 'preserve', 'preempt', 'key', self.hostid, 'skey', oldkey, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to preempt reservation for disk %s" % disk)
        return ret

