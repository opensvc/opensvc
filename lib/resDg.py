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
"""Module providing Generic device group resources
"""

import resources as Res
import rcStatus
import rcExceptions as exc
from rcGlobalEnv import rcEnv

class Dg(Res.Resource):
    """ basic Dg resource, must be extend for LVM / Veritas / ZFS
    """
    def __init__(self, rid=None, name=None, type=None, always_on=set([]), optional=False,
                 disabled=False):
        Res.Resource.__init__(self, rid, type, optional, disabled)
        self.name = name
        self.always_on = always_on
        self.disks = set()

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self),\
                                    self.name)

    def disklist(self):
        return self.disks

    def has_it(self): return False
    def is_up(self): return False
    def do_start(self): return False
    def do_stop(self): return False

    def stop(self):
        self.do_stop()

    def start(self):
        self.do_start()

    def startstandby(self):
        if rcEnv.nodename in self.always_on:
             self.start()

    def _status(self, verbose=False):
        if rcEnv.nodename in self.always_on:
            if self.is_up(): return rcStatus.STDBY_UP
            else: return rcStatus.STDBY_DOWN
        else:
            if self.is_up(): return rcStatus.UP
            else: return rcStatus.DOWN

if __name__ == "__main__":
    for c in (Dg,) :
        help(c)

    print """d=Dg("aGenericDg")"""
    d=Dg("aGenericDg")
    print "show d", d
    print """d.do_action("start")"""
    d.do_action("start")
    print """d.do_action("stop")"""
    d.do_action("stop")
