#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import resDg
import os
import rcStatus
import re
import pwd
import grp
import stat

class Vg(resDg.Dg):
    def __init__(self, rid=None, devs=set([]), user="root",
                 group="root", perm="660", type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False):
        self.label = "raw"
        resDg.Dg.__init__(self, rid=rid, name="raw",
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags,
                          monitor=monitor)

        self.devs = set([])
        self.devs_not_found = set([])
        self.user = user
        self.group = group
        self.perm = perm
        
        for dev in devs:
            if os.path.exists(dev):
                self.devs.add(dev)
            else:
                self.devs_not_found.add(dev)

        self.get_uid()
        self.get_gid()

    def on_add(self):
        try:
            n = self.rid.split('#')[1]
        except:
            n = "0"
        self.name = self.svc.svcname+".raw"+n
        self.label = self.name

    def get_uid(self):
        self.uid = self.user
        if isinstance(self.uid, (str, unicode)):
            try:
                info=pwd.getpwnam(self.uid)
                self.uid = info[2]
            except:
                pass

    def get_gid(self):
        self.gid = self.group
        if isinstance(self.gid, (str, unicode)):
            try:
                info=grp.getgrnam(self.gid)
                self.gid = info[2]
            except:
                pass

    def check_uid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        uid = os.stat(rdev).st_uid
        if uid != self.uid:
            if verbose:
                self.status_log('%s uid should be %d but is %d'%(rdev, self.uid, uid))
            return False
        return True

    def check_gid(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        gid = os.stat(rdev).st_gid
        if gid != self.gid:
            if verbose:
                self.status_log('%s gid should be %d but is %d'%(rdev, self.gid, gid))
            return False
        return True

    def check_perm(self, rdev, verbose=False):
        if not os.path.exists(rdev):
            return True
        try:
            perm = oct(stat.S_IMODE(os.stat(rdev).st_mode))
        except:
            self.log.error('%s can not stat file'%rdev)
            return False
        perm = str(perm).lstrip("0")
        if perm != str(self.perm):
            if verbose:
                self.status_log('%s perm should be %s but is %s'%(rdev, str(self.perm), perm))
            return False
        return True

    def has_it(self):
        """Returns True if all devices are present
        """
        if len(self.devs_not_found) > 0:
            self.status_log("%s not found"%', '.join(self.devs_not_found))
            return False
        return True

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        return self.has_it()

    def _status(self, verbose=False):
        if self.is_up():
            return rcStatus.NA
        else:
            return rcStatus.WARN

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def disklist(self):
        return self.devs
