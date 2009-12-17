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

def scsireserv_supported():
    if which('sg_persist') is None:
        return False
    return True

def ack_unit_attention(self, d):
    i = self.preempt_timeout
    while i>0:
        i -= 1
        cmd = [ 'sg_persist', '-n', '-r', d ]
        (ret, out) = self.call(cmd, errlog=False)
        if "Unit Attention" in out or ret != 0:
            self.log.info("disk %s reports 'Unit Attention' ... waiting" % d)
            time.sleep(1)
            continue
        break
    if i == 0:
        self.log.error("timed out waiting for 'Unit Attention' to go away on disk %s" % d)
        return 1
    return 0

def ack_all_unit_attention(self):
    for d in self.disks:
        if ack_unit_attention(self, d) != 0:
            return 1
    return 0

def disk_registered(self, disk):
    cmd = [ 'sg_persist', '-n', '-k', disk ]
    (ret, out) = self.call(cmd)
    if ret != 0:
        self.log.error("failed to read registrations for disk %s" % disk)
    if self.hostid in out:
        return True
    return False

def disk_register(self, disk):
    cmd = [ 'sg_persist', '-n', '--out', '--register-ignore', '--param-sark='+self.hostid, disk ]
    (ret, out) = self.vcall(cmd)
    if ret != 0:
        self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
    return ret

def disk_unregister(self, disk):
    cmd = [ 'sg_persist', '-n', '--out', '--register-ignore', '--param-rk='+self.hostid, disk ]
    (ret, out) = self.vcall(cmd)
    if ret != 0:
        self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
    return ret

def register(self):
    r = 0
    for d in self.disks:
        r += ack_unit_attention(self, d)
        r += disk_register(self, d)
    return r

def unregister(self):
    r = 0
    for d in self.disks:
        r += ack_unit_attention(self, d)
        if not disk_registered(self, d):
            continue
        r += disk_unregister(self, d)
    return r

def get_reservation_key(self, disk):
    cmd = [ 'sg_persist', '-n', '-r', disk ]
    (ret, out) = self.call(cmd)
    if ret != 0:
        self.log.error("failed to list reservation for disk %s" % disk)
    if 'Key=' not in out:
        return None
    for w in out.split():
        if 'Key=' in w:
            return w.split('=')[1]
    raise Exception()

def disk_reserved(self, disk):
    cmd = [ 'sg_persist', '-n', '-r', disk ]
    (ret, out) = self.call(cmd)
    if ret != 0:
        self.log.error("failed to read reservation for disk %s" % disk)
    if self.hostid in out:
        return True
    return False

def disk_release(self, disk):
    cmd = [ 'sg_persist', '-n', '--out', '--release', '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
    (ret, out) = self.vcall(cmd)
    if ret != 0:
        self.log.error("failed to release disk %s" % disk)
    return ret

def disk_reserve(self, disk):
    cmd = [ 'sg_persist', '-n', '--out', '--reserve', '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
    (ret, out) = self.vcall(cmd)
    if ret != 0:
        self.log.error("failed to reserve disk %s" % disk)
    return ret

def disk_preempt_reservation(self, disk, oldkey):
    cmd = [ 'sg_persist', '-n', '--out', '--preempt-abort', '--param-sark='+oldkey, '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
    (ret, out) = self.vcall(cmd)
    if ret != 0:
        self.log.error("failed to preempt reservation for disk %s" % disk)
    return ret

def disk_wait_reservation(self, disk):
    i = 0
    while i>0:
        i -= 1
        if disk_reserved(disk):
            self.log.info("reservation acquired for disk %s" % disk)
            return 0
        time.sleep(1)
    self.log.error("timed out waiting for reservation for disk %s" % disk)
    return 1

def reserve(self):
    r = 0
    for d in self.disks:
        r += ack_unit_attention(self, d)
        key = get_reservation_key(self, d)
        if key is None:
            r += disk_reserve(self, d)
        elif key == self.hostid:
            continue
        else:
            r += disk_preempt_reservation(self, d, key)
            r += disk_wait_reservation(self, d)
    return r

def release(self):
    r = 0
    for d in self.disks:
        r += ack_unit_attention(self, d)
        if not disk_reserved(self, d):
            continue
        r += disk_release(self, d)
    return r

def checkreserv(self):
    if ack_all_unit_attention(self) != 0:
        return rcStatus.WARN
    r = rcStatus.Status()
    for d in self.disks:
        key = get_reservation_key(self, d)
        if key is None:
            self.log.debug("disk %s is not reserved" % d)
            r += rcStatus.WARN
        elif key != self.hostid:
            self.log.debug("disk %s is reserved by another host whose key is %s" % (d, key))
            r += rcStatus.DOWN
        else:
            self.log.debug("disk %s is correctly reserved" % d)
            r += rcStatus.UP
    return r.status


class ScsiReserv(Res.Resource):
    """Define method to acquire and release scsi SPC-3 persistent reservations
    on disks held by a service
    """
    def __init__(self, disks):
        self.hostid = '0x'+str(uuid.getnode())
        self.disks = disks
        self.preempt_timeout = 10
        self.prtype = '5'
        Res.Resource.__init__(self, "scsireserv")

    def scsireserv(self):
        r = 0
        r += register(self)
        r += reserve(self)
        return r

    def scsirelease(self):
        r = 0
        r += release(self)
        r += unregister(self)
        return r

    def scsicheckreserv(self):
        return checkreserv(self)

    def status(self):
        return self.scsicheckreserv()

    def start(self):
        if self.scsireserv() != 0:
            raise ex.excError

    def stop(self):
        if self.scsirelease() != 0:
            raise ex.excError
