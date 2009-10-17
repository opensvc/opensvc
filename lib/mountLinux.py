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

import logging
import os

import rcStatus
import rcMounts
import mount
from rcUtilities import process_call_argv

class Mount(mount.Mount):
    """ define Linux mount/umount doAction """
    def __init__(self, mountPoint, device, fsType, mntOpt):
        self.Mounts = rcMounts.Mounts()
        mount.Mount.__init__(self, mountPoint, device, fsType, mntOpt)

    def is_up(self):
        if self.Mounts.has_mount(self.device, self.mountPoint) != 0:
            return False
        return True

    def status(self):
        if self.is_up(): return rcStatus.UP
        else: return rcStatus.DOWN

    def start(self):
        log = logging.getLogger('MOUNT')
        if self.is_up() is True:
            log.info("fs(%s %s) is already mounted"%
                (self.device, self.mountPoint))
            return 0
        if not os.path.exists(self.mountPoint):
            os.mkdir(self.mountPoint, 0755)
        cmd = ['mount', '-t', self.fsType, '-o', self.mntOpt, self.device,
self.mountPoint]
        log.info(' '.join(cmd))
        (ret, out) = process_call_argv(cmd)
        return ret

    def stop(self):
        log = logging.getLogger('UMOUNT')
        if self.is_up() is False:
            log.info("fs(%s %s) is already umounted"%
                    (self.device, self.mountPoint))
            return 0
        cmd = ['umount', self.mountPoint]
        log.info(' '.join(cmd))
        (ret, out) = process_call_argv(cmd)
        if ret != 0:
            log.error("failed")
            return 1
        return 0

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

