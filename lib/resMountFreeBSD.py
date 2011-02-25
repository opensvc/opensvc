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

import rcMountsFreeBSD as rcMounts
import resMount as Res
from rcUtilities import qcall, protected_mount, getmount
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
        nb_killed = self.killfuser(self.mountPoint)
        self.log.info('umount %s'%self.mountPoint)
        cmd = ['umount', self.mountPoint]
        ret = qcall(cmd)
        if ret == 0 or nb_killed == 0:
            break

    if ret != 0:
        self.log.info("no more process using %s, yet umount fails. try forced umount."%self.mountPoint)
        cmd = ['umount', '-f', self.mountPoint]
        (ret, out) = self.vcall(cmd)

    return ret


class Mount(Res.Mount):
    """ define FreeBSD mount/umount doAction """
    def __init__(self, rid, mountPoint, device, fsType, mntOpt,
                 snap_size=None, always_on=set([]),
                 disabled=False, tags=set([]), optional=False):
        self.Mounts = None
        Res.Mount.__init__(self, rid, mountPoint, device, fsType, mntOpt,
                           snap_size, always_on,
                           disabled=disabled, tags=tags, optional=optional)
        self.fsck_h = {
            'ufs': {'bin': 'fsck', 'cmd': ['fsck', '-t', 'ufs', '-p', self.device]},
        }

    def killfuser(self, dir):
        cmd = ['fuser', '-kmc', dir]
        (ret, out) = self.vcall(cmd, err_to_info=True)

        """ return the number of process we sent signal to
        """
        l = out.split(':')
        if len(l) < 2:
            return 0
        return len(l[1].split())

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def realdev(self):
        dev = None
        try:
            mode = os.stat(self.device)[ST_MODE]
        except:
            self.log.debug("can not stat %s" % self.device)
            return None
        if S_ISCHR(mode):
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

    def disklist(self):
        dev = self.realdev()
        if dev is None:
            return set([])

        try:
            statinfo = os.stat(dev)
        except:
            self.log.error("can not stat %s" % dev)
            raise ex.excError

        return set([dev])

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

