#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
import os
import logging

from rcGlobalEnv import rcEnv
from rcUtilities import which
import rcExceptions as ex
import rcStatus
import resources as Res
import time
import datetime
import resSyncSymclone as symclone

class syncSymclone(symclone.syncSymclone):
    def dev_rescan(self, dev):
        dev = dev.replace('/dev/', '')
        sysdev = "/sys/block/%s/device/rescan"%dev
        self.log.info("echo 1>%s"%sysdev)
        with open(sysdev, 'w') as s:
            s.write("1")

    def refresh_multipath(self, dev):
        cmd = ['multipath', '-v0', '-r', dev]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def dev_ready(self, dev):
        cmd = ['sg_turs', dev]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            return False
        return True

    def wait_for_dev_ready(self, dev):
        delay = 1
        timeout = 5
        for i in range(timeout/delay):
            if self.dev_ready(dev):
                return
            if i == 0:
                self.log.info("waiting for device %s to become ready (max %i secs)"%(dev,timeout))
            time.sleep(delay)
        self.log.error("timed out waiting for device %s to become ready (max %i secs)"%(dev,timeout))
        raise ex.excError

    def wait_for_devs_ready(self):
        for (symid, devid) in self.symld:
            dev = self.symld[symid, devid]['pdev']
            self.dev_rescan(dev)
            self.wait_for_dev_ready(dev)
            self.refresh_multipath(dev)

    def __init__(self,
                 rid=None,
                 symdg=None,
                 symdevs=[],
                 precopy_timeout=300,
                 sync_max_delay=1440,
                 sync_min_delay=30,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 internal=False,
                 subset=None):
        symclone.syncSymclone.__init__(self,
                                       rid,
                                       symdg,
                                       symdevs,
                                       precopy_timeout,
                                       sync_max_delay,
                                       sync_min_delay,
                                       optional=optional,
                                       disabled=disabled,
                                       tags=tags,
                                       internal=internal,
                                       subset=subset)

