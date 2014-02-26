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
    def __init__(self,
                 rid=None,
                 name=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 restart=0,
                 subset=None,
                 tags=set([])):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.sg",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          tags=tags,
                          always_on=always_on)
        self.label = name

    def __status(self, verbose=False):
        if 'node' in self.svc.cmviewcl and \
           rcEnv.nodename in self.svc.cmviewcl['node'] and \
           'status' in self.svc.cmviewcl['node'][rcEnv.nodename] and \
           self.svc.cmviewcl['node'][rcEnv.nodename]['status'] == "up":
            return rcStatus.UP
        return rcStatus.DOWN
