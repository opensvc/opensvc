#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self, rid=None, name=None, always_on=set([]),
                 optional=False, disabled=False, tags=set([])):
        resHb.Hb.__init__(self, rid, "hb.rhcs",
                          optional=optional, disabled=disabled, tags=tags)
        self.label = name

    def _status(self, verbose=False):
        marker = 'service:'+self.svc.pkg_name
        for line in self.svc.clustat:
            l = line.split()
            if len(l) < 3:
                continue
            if marker != l[0].strip():
                continue

            # package found
            if rcEnv.nodename != self.svc.member_to_nodename(l[1].strip()):
                return rcStatus.DOWN
            elif l[-1].strip() != "started":
                return rcStatus.DOWN
            return rcStatus.UP

        # package not found
        return rcStatus.DOWN
