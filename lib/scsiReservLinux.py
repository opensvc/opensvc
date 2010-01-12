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

def scsireserv_supported():
    if which('sg_persist') is None:
        return False
    return True


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

    def ack_unit_attention(self, d):
        i = self.preempt_timeout
        while i>0:
            i -= 1
            cmd = [ 'sg_persist', '-n', '-r', d ]
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            out = p.communicate()
            ret = p.returncode
            if "unsupported service action" in out[1]:
                self.log.error("disk %s does not support persistent reservation" % d)
                raise ex.excScsiPrNotsupported
            if "Unit Attention" in out[0] or ret != 0:
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
            if self.ack_unit_attention(d) != 0:
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
            try:
                r += self.ack_unit_attention(d)
                r += self.disk_register(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def unregister(self):
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                if not self.disk_registered(d):
                    continue
                r += self.disk_unregister(d)
            except ex.excScsiPrNotsupported:
                continue
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
            if self.disk_reserved(disk):
                self.log.info("reservation acquired for disk %s" % disk)
                return 0
            time.sleep(1)
        self.log.error("timed out waiting for reservation for disk %s" % disk)
        return 1

    def reserve(self):
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                key = self.get_reservation_key(d)
                if key is None:
                    r += self.disk_reserve(d)
                elif key == self.hostid:
                    continue
                else:
                    r += self.disk_preempt_reservation(d, key)
                    r += self.disk_wait_reservation(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def release(self):
        r = 0
        for d in self.disks:
            try:
                r += self.ack_unit_attention(d)
                if not self.disk_reserved(d):
                    continue
                r += self.disk_release(d)
            except ex.excScsiPrNotsupported:
                continue
        return r

    def checkreserv(self):
        if self.ack_all_unit_attention() != 0:
            return rcStatus.WARN
        r = rcStatus.Status()
        for d in self.disks:
            try:
                key = self.get_reservation_key(d)
                if key is None:
                    self.log.debug("disk %s is not reserved" % d)
                    r += rcStatus.WARN
                elif key != self.hostid:
                    self.log.debug("disk %s is reserved by another host whose key is %s" % (d, key))
                    r += rcStatus.DOWN
                else:
                    self.log.debug("disk %s is correctly reserved" % d)
                    r += rcStatus.UP
            except ex.excScsiPrNotsupported:
                continue
        return r.status

    def scsireserv(self):
        r = 0
        r += self.register()
        r += self.reserve()
        return r

    def scsirelease(self):
        r = 0
        r += self.release()
        r += self.unregister()
        return r

    def scsicheckreserv(self):
        return self.checkreserv()

    def status(self):
        return self.scsicheckreserv()

    def start(self):
        if self.scsireserv() != 0:
            raise ex.excError

    def stop(self):
        if self.scsirelease() != 0:
            raise ex.excError
