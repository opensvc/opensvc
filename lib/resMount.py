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
import os
import rcExceptions as ex
import rcStatus
from rcGlobalEnv import rcEnv
from rcUtilities import which

class Mount(Res.Resource):
    """Define a mount resource 
    """
    def __init__(self, rid=None, mountPoint=None, device=None, fsType=None,
                 mntOpt=None, always_on=set([]), optional=False,
                 disabled=False):
        Res.Resource.__init__(self, rid, "fs", optional, disabled)
        self.mountPoint = mountPoint
        self.device = device
        self.fsType = fsType
        self.mntOpt = mntOpt
        self.always_on = always_on
        self.label = device + '@' + mountPoint
        self.fsck_h = {}
        self.testfile = os.path.join(mountPoint, '.opensvc')

    def start(self):
        if not os.path.exists(self.mountPoint):
            try:
                os.makedirs(self.mountPoint)
            except:
                self.log.info("failed to create missing mountpoint %s" % self.mountPoint)
                raise
            self.log.info("create missing mountpoint %s" % self.mountPoint)

    def startstandby(self):
        if rcEnv.nodename in self.always_on:
             self.start()

    def fsck(self):
        if self.fsType not in self.fsck_h:
            self.log.info("fsck not implemented for %s"%self.fsType)
            return
        bin = self.fsck_h[self.fsType]['bin']
        cmd = self.fsck_h[self.fsType]['cmd']
        if which(bin) is None:
            self.log.warning("%s not found. bypass."%self.fsType)
            return
        (ret, out) = self.vcall(cmd)
        if ret != 0: 
            raise ex.excError 

    def need_check_writable(self):
        if 'ro' in self.mntOpt.split(','):
            return False
        if 'nfs' in self.fsType:
            return False
        return True

    def can_check_writable(self):
        """ orverload in child classes to check os-specific conditions
            when a write test might hang (solaris lockfs, linux multipath
            with queueing on and no active path)
        """
        return True

    def check_writable(self):
        if not self.can_check_writable():
            return False
        try:
            f = open(self.testfile, 'w')
            f.write(' ')
            f.close()
        except:
            return False
        return True

    def _status(self, verbose=False):
        if rcEnv.nodename in self.always_on:
            if self.is_up():
                if self.need_check_writable() and not self.check_writable():
                    svc.status_log("fs is not writable")
                    return rcStatus.WARN
                return rcStatus.STDBY_UP
            else:
                return rcStatus.STDBY_DOWN
        else:
            if self.is_up():
                if self.need_check_writable() and not self.check_writable():
                    svc.status_log("fs is not writable")
                    return rcStatus.WARN
                return rcStatus.UP
            else:
                return rcStatus.DOWN

    def __str__(self):
        return "%s mnt=%s dev=%s fsType=%s mntOpt=%s" % (Res.Resource.__str__(self),\
                self.mountPoint, self.device, self.fsType, self.mntOpt)

    def __cmp__(self, other):
        """order so that deepest mountpoint can be umount first
        """
        return cmp(self.mountPoint, other.mountPoint)

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)
    print """   m=Mount("/mnt1","/dev/sda1","ext3","rw")   """
    m=Mount("/mnt1","/dev/sda1","ext3","rw")
    print "show m", m


