#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@free.fr>'
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
from datetime import datetime
import rcStatus
import resources as Res
import time
import os
from rcUtilities import justcall, vcall
from stat import *
import resContainer

class Zone(resContainer.Container):

    def __init__(self, name, optional=False, disabled=False, tags=set([])):
        """define Zone object attribute :
                name
                label
                state
                zonepath
        """
        Res.Resource.__init__(self, rid="zone", type="container.zone",
                              optional=optional, disabled=disabled, tags=tags)
        self.name = name
        self.label = name
        self.state = None
        self.zonepath = os.path.realpath(os.path.join( 'zones', self.name))
        self.zone_refresh()

    def zoneadm(self, action):
        if action in [ 'ready' , 'boot' ,'shutdown' , 'halt' ,'attach', 'detach' ] :
            cmd = ['zoneadm', '-z', self.name, action ]
        else:
            self.log.error("unsupported zone action: %s" % action)
            return 1

        t = datetime.now()
        (ret, out) = self.vcall(cmd)
        len = datetime.now() - t
        self.log.info('%s done in %s - ret %i - logs in %s'
                    % (action, len, ret, out))
        return ret

    def set_zonepath_perms(self):
        if not os.path.exists(self.zonepath):
            os.makedirs(self.zonepath)
        s = os.stat(self.zonepath)
        if s.st_uid != 0 or s.st_gid != 0:
            self.log.info("set %s ownership to uid 0 gid 0"%self.zonepath)
            os.chown(self.zonepath, 0, 0)
        mode = s[ST_MODE]
        if (S_IWOTH&mode) or (S_IXOTH&mode) or (S_IROTH&mode) or \
           (S_IWGRP&mode) or (S_IXGRP&mode) or (S_IRGRP&mode):
            self.vcall(['chmod', '700', self.zonepath])

    def attach(self):
        self.zone_refresh()
        if self.state == "installed" :
            self.log.info("zone container %s already installed" % self.name)
            return 0
        return self.zoneadm('attach')

    def detach(self):
        self.zone_refresh()
        if self.state == "configured" :
            self.log.info("zone container %s already detached/configured" % self.name)
            return 0
        return self.zoneadm('detach')

    def ready(self):
        self.zone_refresh()
        if self.state == 'ready' or self.state == "running" :
            self.log.info("zone container %s already ready" % self.name)
            return 0
        self.set_zonepath_perms()
        return self.zoneadm('ready')

    def boot(self):
        self.zone_refresh()
        if self.state == "running" :
            self.log.info("zone container %s already running" % self.name)
            return 0
        return self.zoneadm('boot')

    def stop(self):
        """ Need wait poststat after returning to installed state on ipkg
            example : /bin/ksh -p /usr/lib/brand/ipkg/poststate zonename zonepath 5 4
        """
        self.zone_refresh()
        if self.state == 'installed' :
            self.log.info("zone container %s already stopped" % self.name)
            return 0
        if self.state == 'running':
            (ret, out) = self.vcall(['zlogin' , self.name , '/sbin/init' , '0'])
            for t in range(self.shutdown_timeout):
                self.zone_refresh()
                if self.state == 'installed':
                    for t2 in range(self.shutdown_timeout):
                        time.sleep(1)
                        (out,err,st) = justcall([ 'pgrep', '-fl', 'ipkg/poststate.*'+ self.name])
                        if st == 0 : 
                            self.log.info("Waiting for ipkg poststate complete: %s" % out)
                        else:
                            break
                    return 0
                time.sleep(1)
            self.log.info("timeout out waiting for %s shutdown", self.name)
        return self.zoneadm('halt')

    def _status(self, verbose=False):
        self.zone_refresh()
        if self.state == 'running' :
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def zone_refresh(self):
        """ refresh Zone object attributes:
                state
                zonepath
                brand
            from zoneadm -z zonename list -p
            zoneid:zonename:state:zonepath:uuid:brand:ip-type
        """

        (out,err,st) = justcall([ 'zoneadm', '-z', self.name, 'list', '-p' ])

        if st == 0 :
            (zoneid,zonename,state,zonepath,uuid,brand,iptype)=out.split(':')
            if zonename == self.name :
                self.state = state
                self.zonepath = zonepath
                self.brand = brand
                return True
            else:
                return False
        else:
            return False

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)


if __name__ == "__main__":
    for c in (Zone,) :
        help(c)
