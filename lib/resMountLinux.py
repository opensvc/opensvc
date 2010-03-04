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

import rcStatus
import rcMountsLinux as rcMounts
import resMount as Res
from rcUtilities import qcall, protected_mount, getmount
from rcUtilitiesLinux import major, get_blockdev_sd_slaves
from rcGlobalEnv import rcEnv
import rcExceptions as ex
from stat import *

def try_umount(self):
    cmd = ['umount', self.mountPoint]
    (ret, out) = self.vcall(cmd, err_to_warn=True)
    if ret == 0:
        return 0

    """ don't try to kill process using the source of a 
        protected bind mount
    """
    if protected_mount(self.mountPoint):
        return 1

    """ best effort kill of all processes that might block
        the umount operation. The priority is given to mass
        action reliability, ie don't contest oprator's will
    """
    cmd = ['sync']
    (ret, out) = self.vcall(cmd)

    for i in range(4):
        cmd = ['fuser', '-kmv', self.mountPoint]
        (ret, out) = self.vcall(cmd, err_to_warn=True)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Mount(Res.Mount):
    """ define Linux mount/umount doAction """
    def __init__(self, rid, mountPoint, device, fsType, mntOpt, always_on=set([])):
        self.Mounts = None
        Res.Mount.__init__(self, rid, mountPoint, device, fsType, mntOpt, always_on)
        self.fsck_h = {
            'ext2': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device]},
            'ext3': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device]},
            'ext4': {'bin': 'e2fsck', 'cmd': ['e2fsck', '-p', self.device]},
        }

    def is_up(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def status(self):
        if rcEnv.nodename in self.always_on:
            if self.is_up(): return rcStatus.STDBY_UP
            else: return rcStatus.STDBY_DOWN
        else:
            if self.is_up(): return rcStatus.UP
            else: return rcStatus.DOWN

    def disklist(self):
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return set([])
        if S_ISBLK(mode):
            dev = self.device
        else:
            mnt = getmount(self.device)
            if self.Mounts is None:
                self.Mounts = rcMounts.Mounts()
            m = self.Mounts.has_param("mnt", mnt)
            if m is None:
                self.log.error("can't find the hosting device of %(mnt)s (hosting %(dev)s) in mnttab"%dict(mnt=mnt, dev=dev))
                raise ex.excError
            dev = m.dev

        try:
            dm_major = major('device-mapper')
        except:
            return set([dev])

        try:
            statinfo = os.stat(dev)
        except:
            self.log.error("can not stat %s" % dev)
            raise ex.excError
        if os.major(statinfo.st_rdev) != dm_major:
            return set([dev])
        dm = 'dm-' + str(os.minor(statinfo.st_rdev))
        syspath = '/sys/block/' + dm + '/slaves'
        devs = get_blockdev_sd_slaves(syspath)
        return devs

    def start(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        Res.Mount.start(self)
        if self.is_up() is True:
            self.log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return 0
        self.fsck()
        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0755)
        if self.fsType != "":
            fstype = ['-t', self.fsType]
        else:
            fstype = []
        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []
        cmd = ['mount']+fstype+mntopt+[self.device, self.mountPoint]
        (ret, out) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.Mounts = None

    def stop(self):
        if self.Mounts is None:
            self.Mounts = rcMounts.Mounts()
        if self.is_up() is False:
            self.log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return
        for i in range(3):
            ret = try_umount(self)
            if ret == 0: break
        if ret != 0:
            self.log.error('failed to umount %s'%self.mountPoint)
            raise ex.excError
        self.Mounts = None

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

