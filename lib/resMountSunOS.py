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
"Module implement SunOS specific mounts"

import os

import rcStatus
import rcMountsSunOS as rcMounts
import resMount as Res
import rcExceptions as ex
from rcZfs import zfs_getprop, zfs_setprop
from rcUtilities import justcall

class Mount(Res.Mount):
    """ define SunOS mount/umount doAction """
    def __init__(self, rid, mountPoint, device, fsType, mntOpt,
                 snap_size=None, always_on=set([]),
                 disabled=False, tags=set([]), optional=False,
                 monitor=False, restart=0):
        self.rdevice = device.replace('/dsk/','/rdsk/',1)
        self.Mounts = rcMounts.Mounts()
        Res.Mount.__init__(self, rid, mountPoint, device, fsType, mntOpt,
                           snap_size, always_on,
                           disabled=disabled, tags=tags, optional=optional,
                           monitor=monitor, restart=restart)
        self.fsck_h = {
            'ufs': {'bin': 'fsck',
                    'cmd':       ['fsck', '-F', 'ufs', '-y', self.rdevice],
                    'reportcmd': ['fsck', '-F', 'ufs', '-m', self.rdevice],
                    'reportclean': [ 32 ],
            },
            'vxfs': {'bin': 'fsck',
                    'cmd':       ['fsck', '-F', 'vxfs', '-y', self.rdevice],
                    'reportcmd': ['fsck', '-F', 'vxfs', '-m', self.rdevice],
                    'reportclean': [ 32 ],
            },
        }

    def is_up(self):
        self.Mounts = rcMounts.Mounts()
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def start(self):
        self.Mounts = None
        Res.Mount.start(self)

        if self.is_up() is True:
            self.log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return

        if self.fsType == 'zfs' :
            if zfs_getprop(self.device, 'mountpoint' ) != self.mountPoint :
                if zfs_setprop(self.device, 'zoned', 'off') :
                    if not zfs_setprop(self.device, 'mountpoint', self.mountPoint) :
                        raise ex.excError

            self.Mounts = None
            if self.is_up() is True:
                return

            (stdout,stderr,returncode)= justcall(['rm', self.mountPoint+"/.opensvc" ])
            ret, out, err = self.vcall(['zfs', 'mount', self.device ])
            if ret != 0:
                ret, out, err = self.vcall(['zfs', 'mount', '-O', self.device ])
                if ret != 0:
                    raise ex.excError
            return
        elif self.fsType != "":
            fstype = ['-F', self.fsType]
            self.fsck()
        else:
            fstype = []

        if self.mntOpt != "":
            mntopt = ['-o', self.mntOpt]
        else:
            mntopt = []

        if not os.path.exists(self.mountPoint):
            os.makedirs(self.mountPoint, 0o755)
        cmd = ['mount']+fstype+mntopt+[self.device, self.mountPoint]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.Mounts = None
            raise ex.excError
        self.Mounts = None
        self.can_rollback = True

    def try_umount(self):
        if self.fsType == 'zfs' :
            ret, out, err = self.vcall(['zfs', 'umount', self.device ], err_to_info=True)
            if ret != 0 :
                ret, out, err = self.vcall(['zfs', 'umount', '-f', self.device ], err_to_info=True)
                if ret != 0 :
                    raise ex.excError
            return
        (ret, out, err) = self.vcall(['umount', self.mountPoint], err_to_info=True)
        if ret == 0 :
            return
        for i in range(4):
            (ret, out, err) = self.vcall(['fuser', '-ck', self.mountPoint],
                                    err_to_info=True)
            (ret, out, err) = self.vcall(['umount', self.mountPoint],
                                    err_to_info=True)
            if ret == 0 :
                return
            if self.fsType != 'lofs' :
                (ret, out, err) = self.vcall(['umount', '-f', self.mountPoint],
                                        err_to_info=True)
                if ret == 0 :
                    return
        raise ex.excError

    def stop(self):
        self.Mounts = None
        if self.is_up() is False:
            self.log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return

        try: 
            self.try_umount()
        except:
            self.Mounts = None
            self.log.error("failed")
            raise ex.excError
        self.Mounts = None

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

