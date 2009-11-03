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
import mount

class Mount(mount.Mount):
    """ define SunOS mount/umount doAction """
    def __init__(self, mountPoint, device, fsType, mntOpt):
        self.Mounts = rcMounts.Mounts()
        mount.Mount.__init__(self, mountPoint, device, fsType, mntOpt)

    def is_up(self):
        return self.Mounts.has_mount(self.device, self.mountPoint)

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def start(self):
        if self.is_up() is True:
            self.log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return 0
        if not os.path.exists(self.mountPoint):
            os.mkdir(self.mountPoint, 0755)
        cmd = ['mount', '-F', self.fsType, '-o', self.mntOpt, self.device, \
            self.mountPoint]
        (ret, out) = self.vcall(cmd)
        return ret

    def stop(self):
        if self.is_up() is False:
            self.log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return 0

        (ret, out) = self.vcall(['umount', self.mountPoint])
        if ret == 0 :
            return 0

        if self.fsType != 'lofs' :
            (ret, out) = self.vcall(['umount', '-f', self.mountPoint])
            if ret == 0 :
                return 0
        
        self.log.error("failed")
        return 1
        

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

