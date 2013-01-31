#
# Copyright (c) 2013 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcStatus
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Hb(resHb.Hb):
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        resHb.Hb.__init__(self, rid, "hb.vcs",
                          optional=optional, disabled=disabled, tags=tags)
        self.label = name

    def _status(self, verbose=False):
        try:
            s = self.svc.get_grp_val('State').strip('|')
        except ex.excError, e:
            self.status_log(str(e))
            return rcStatus.WARN

        if s == "ONLINE":
            return rcStatus.UP
        elif s == "OFFLINE":
            return rcStatus.DOWN
        else:
            self.status_log(s)
            return rcStatus.WARN

