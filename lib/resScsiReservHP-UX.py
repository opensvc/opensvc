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
import os
import time
import rcStatus
import rcExceptions as ex
from rcUtilities import which
from subprocess import *
import resScsiReserv

def mpath_to_path(disks):
    l = []
    for disk in disks:
        if "/dev/dsk" in disk:
            l.append(disk.replace("/dev/dsk", "/dev/rdsk"))
            continue
        if "/dev/disk" not in disk and "/dev/rdisk" not in disk:
            continue
        if not os.path.exists(disk):
            continue
        cmd = ['ioscan', '-F', '-m', 'dsf', disk]
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        buff = p.communicate()
        ret = p.returncode
        if ret != 0:
            continue
        a = buff[0].split(':')
        if len(a) != 2:
            continue
        b = a[1].split()
        for d in b:
            l.append(d.replace("/dev/dsk", "/dev/rdsk"))
    return l
    
class ScsiReserv(resScsiReserv.ScsiReserv):
    def __init__(self, rid=None, disks=set([])):
        resScsiReserv.ScsiReserv.__init__(self, rid, disks)
        self.prtype = 'wero'
        self.disks = mpath_to_path(disks)
        self.leg_mpath_disable()

    def scsireserv_supported(self):
        if which('scu') is None:
            return False
        return True

    def leg_mpath_disable(self):
        cmd = ['scsimgr', 'get_attr', '-p', '-a', 'leg_mpath_enable']
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        buff = p.communicate()
        ret = p.returncode
        if ret != 0:
            self.log.error("can not fetch 'leg_mpath_enable' value")
            raise ex.excError
        if 'false' in buff[0]:
            return
        cmd = ['scsimgr', 'save_attr', '-a', 'leg_mpath_enable=false']
        self.log.info(' '.join(cmd))
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        buff = p.communicate()
        ret = p.returncode
        if ret != 0:
            self.log.error("can not set 'leg_mpath_enable' value")
            raise ex.excError

    def ack_unit_attention(self, d):
        return 0

    def disk_registered(self, disk):
        cmd = [ 'scu', '-f', disk, 'show', 'keys' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_register(self, disk):
        cmd = [ 'scu', '-f', disk, 'preserve', 'register', 'skey', self.hostid ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        cmd = [ 'scu', '-f', disk, 'preserve', 'register', 'skey', '0', 'key', self.hostid ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

    def get_reservation_key(self, disk):
        cmd = [ 'scu', '-f', disk, 'show', 'reservation' ]
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
        cmd = [ 'scu', '-f', disk, 'show', 'reservation' ]
        (ret, out) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read reservation for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_release(self, disk):
        cmd = [ 'scu', '-f', disk, 'preserve', 'release', 'key', self.hostid, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def disk_reserve(self, disk):
        cmd = [ 'scu', '-f', disk, 'preserve', 'reserve', 'key', self.hostid, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def disk_preempt_reservation(self, disk, oldkey):
        cmd = [ 'scu', '-f', disk, 'preserve', 'preempt', 'key', self.hostid, 'skey', oldkey, 'type', self.prtype ]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to preempt reservation for disk %s" % disk)
        return ret

