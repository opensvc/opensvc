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


class Zone(Res.Resource):
    """
     container Zone status transition diagram :
    """
    shutdown_timeout = 120

    def __init__(self, name, optional=False, disabled=False):
        """define Zone object attribute :
                name
                label
                state
                zonepath
        """
        Res.Resource.__init__(self, rid="zone", type="container.zone",
                              optional=optional, disabled=disabled)
        self.name = name
        self.label = name
        self.state = None
        self.zonepath = os.path.realpath(os.path.join( 'zones', self.name))
        self.zone_refresh()

    def zoneadm(self, action):
        if action in [ 'ready' , 'boot' ,'shutdown' , 'halt' ] :
            cmd = ['zoneadm', '-z', self.name, action ]
        else:
            self.log.error("unsupported lxc action: %s" % action)
            return 1

        t = datetime.now()
        (ret, out) = self.vcall(cmd)
        len = datetime.now() - t
        self.log.info('%s done in %s - ret %i - logs in %s'
                    % (action, len, ret, out))
        return ret

    def ready(self):
        self.zone_refresh()
        if self.state == 'ready' or self.state == "runing" :
            self.log.info("zone container %s already ready" % self.name)
            return 0
        return self.zoneadm('ready')

    def boot(self):
        self.zone_refresh()
        if self.state == "runing" :
            self.log.info("zone container %s already running" % self.name)
            return 0
        return self.zoneadm('boot')

    def stop(self):
        self.zone_refresh()
        if self.state == 'installed' :
            self.log.info("zone container %s already stopped" % self.name)
            return 0
        if self.state == 'running':
            (ret, out) = self.vcall(['zlogin' , self.name , 'init' , '0'])
            for t in range(self.shutdown_timeout):
                self.zone_refresh()
                if self.state == 'installed':
                    return 0
                time.sleep(1)
            self.log.info("timeout out waiting for %s shutdown", self.name)
        return self.zoneadm('halt')

    def status(self):
        self.zone_refresh()
        if self.state == 'running' :
            return rcStatus.UP
        else:
            return rcStatus.Down

    def zone_refresh(self):
        """ refresh Zone object attributes:
                state
                zonepath
            from zoneadm -z zonename list -p
            zoneid:zonename:state:zonepath:uuid:brand:ip-type
        """

        (out,err,st) = justcall([ 'zoneadm', '-z', self.name, 'list', '-p' ])

        if st == 0 :
            (zoneid,zonename,state,zonepath,uuid,brand,iptype)=out.split(':')
            if zonename == self.name :
                self.state = state
                self.zonepath = zonepath
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