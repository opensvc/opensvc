#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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

import rcMountsAIX as rcMounts
import resMount as Res
from rcUtilities import qcall, protected_mount, getmount
from rcGlobalEnv import rcEnv
import rcExceptions as ex
from stat import *

def try_umount(self):
    cmd = ['umount', self.mountPoint]
    (ret, out, err) = self.vcall(cmd, err_to_info=True)
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
    (ret, out, err) = self.vcall(cmd, err_to_info=True)

    for i in range(4):
        cmd = ['fuser', '-k', '-x', '-c', self.mountPoint]
        (ret, out, err) = self.vcall(cmd, err_to_info=True)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0:
            break

    return ret


class Mount(Res.Mount):
    """ define Linux mount/umount doAction """
    def __init__(self, rid, mountPoint, device, fsType, mntOpt,
                 snap_size=None, always_on=set([]),
                 disabled=False, tags=set([]), optional=False,
                 monitor=False):
        self.Mounts = None
        Res.Mount.__init__(self, rid, mountPoint, device, fsType, mntOpt,
                           snap_size, always_on,
                           disabled=disabled, tags=tags, optional=optional,
                           monitor=monitor)
        self.fsck_h = {
            'jfs': {'bin': 'fsck', 'cmd': ['fsck', '-p', '-V', 'jfs', self.device]},
            'jfs2': {'bin': 'fsck', 'cmd': ['fsck', '-p', '-V', 'jfs2', self.device]},
        }

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def realdev(self):
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return None
        if S_ISBLK(mode):
            dev = self.device
        else:
            mnt = getmount(self.device)
            if self.Mounts is None:
                self.Mounts = rcMounts.Mounts()
            m = self.Mounts.has_param("mnt", mnt)
            if m is None:
                self.log.debug("can't find dev %(dev)s mounted in %(mnt)s in mnttab"%dict(mnt=mnt, dev=self.device))
                return None
            dev = m.dev

        return dev

    def mplist(self):
        dev = self.realdev()
        if dev is None:
            return set([])

        return self._mplist([dev])

    def _mplist(self, devs):
        mps = set([])
        return mps

    def disklist(self):
        dev = self.realdev()
        if dev is None:
            return set([])
        return set([])

    def can_check_writable(self):
        return True

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
            fstype = ['-v', self.fsType]
        else:
            fstype = []
        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []
        cmd = ['mount']+fstype+mntopt+[self.device, self.mountPoint]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.Mounts = None
        self.can_rollback = True

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

