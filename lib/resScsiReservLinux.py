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

import os
import resources as Res
import uuid
import re
import time
import rcStatus
import rcExceptions as ex
from rcUtilities import which
from subprocess import *
import resScsiReserv
from rcGlobalEnv import rcEnv


class ScsiReserv(resScsiReserv.ScsiReserv):
    def scsireserv_supported(self):
        if which('sg_persist') is None:
            self.log.debug("sg_persist must be installed to use scsi-3 reservations" )
            return False
        return True

    def set_read_only(self, val):
        if rcEnv.sysname != "Linux":
            return
        os.environ["SG_PERSIST_O_RDONLY"] = str(val)

    def ack_unit_attention(self, d):
        if not os.path.exists(d):
            return 0
        i = self.preempt_timeout
        self.set_read_only(0)
        while i>0:
            i -= 1
            cmd = [ 'sg_persist', '-n', '-r', d ]
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            out = p.communicate()
            ret = p.returncode
            if "unsupported service action" in out[1]:
                self.log.error("disk %s does not support persistent reservation" % d)
                raise ex.excScsiPrNotsupported
            if "error opening file" in out[1]:
                return 0
            if "Unit Attention" in out[0] or ret != 0:
                self.log.debug("disk %s reports 'Unit Attention' ... waiting" % d)
                time.sleep(1)
                continue
            break
        if i == 0:
            self.log.error("timed out waiting for 'Unit Attention' to go away on disk %s" % d)
            return 1
        return 0

    def disk_registered(self, disk):
        self.set_read_only(1)
        cmd = [ 'sg_persist', '-n', '-k', disk ]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_register(self, disk):
        self.set_read_only(0)
        cmd = [ 'sg_persist', '-n', '--out', '--register-ignore', '--param-sark='+self.hostid, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        self.set_read_only(0)
        cmd = [ 'sg_persist', '-n', '--out', '--register-ignore', '--param-rk='+self.hostid, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

    def dev_to_mpath_dev(self, devpath):
        if which("multipath") is None:
            raise ex.excError("multipath not found.")
        cmd = ["multipath", "-l", "-v1", devpath]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError(err)
        _devpath = "/dev/mapper/"+out.strip()
        if not os.path.exists(_devpath):
            raise ex.excError("%s does not exist")
        return _devpath

    def get_reservation_key(self, disk):
        try:
            return self._get_reservation_key(disk)
        except ex.excError as e:
            disk = self.dev_to_mpath_dev(disk)
            return self._get_reservation_key(disk)

    def _get_reservation_key(self, disk):
        self.set_read_only(1)
        cmd = [ 'sg_persist', '-n', '-r', disk ]
        (ret, out, err) = self.call(cmd, errlog=None)
        if ret != 0:
            raise ex.excError("failed to list reservation for disk %s" % disk)
        if 'Key=' not in out:
            return None
        for w in out.split():
            if 'Key=' in w:
                return w.split('=')[1]
        raise Exception()

    def disk_reserved(self, disk):
        try:
            return self._disk_reserved(disk)
        except ex.excError as e:
            disk = self.dev_to_mpath_dev(disk)
            return self._disk_reserved(disk)

    def _disk_reserved(self, disk):
        self.set_read_only(1)
        cmd = [ 'sg_persist', '-n', '-r', disk ]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError("failed to read reservation for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_release(self, disk):
        self.set_read_only(0)
        cmd = [ 'sg_persist', '-n', '--out', '--release', '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def disk_clear_reservation(self, disk):
        cmd = [ 'sg_persist', '-n', '--out', '--clear', '--param-rk='+self.hostid, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to clear reservation on disk %s" % disk)
        return ret

    def disk_reserve(self, disk):
        self.set_read_only(0)
        cmd = [ 'sg_persist', '-n', '--out', '--reserve', '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def _disk_preempt_reservation(self, disk, oldkey):
        m = __import__("rcDiskInfo"+rcEnv.sysname)
        if self.no_preempt_abort or m.diskInfo(deferred=True).disk_vendor(disk).strip() in ["VMware"]:
            preempt_opt = '--preempt'
        else:
            preempt_opt = '--preempt-abort'
        self.set_read_only(0)
        cmd = [ 'sg_persist', '-n', '--out', preempt_opt, '--param-sark='+oldkey, '--param-rk='+self.hostid, '--prout-type='+self.prtype, disk ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to preempt reservation for disk %s" % disk)
        return ret

