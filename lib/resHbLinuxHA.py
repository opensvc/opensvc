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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import resHb
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall, which

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        resHb.Hb.__init__(self, rid, "hb.linuxha",
                          optional=optional, disabled=disabled, tags=tags)
        self.status_cmd = 'cl_status'
        self.name = name

    def process_running(self):
        cmd = [self.status_cmd, 'hbstatus']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return False
        if not 'is running' in out:
            return False
        return True

    def __status(self, verbose=False):
        if not which(self.status_cmd):
            self.status_log("heartbeat is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("heartbeat daemons are not running")
            return rcStatus.WARN
        return rcStatus.NA

