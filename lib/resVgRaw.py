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

class Vg(resDg.Dg):
    def __init__(self, rid=None, devs=set([]), type=None,
                 optional=False, disabled=False, tags=set([]),
                 always_on=set([]), monitor=False):
        self.label = "raw"
        self.devs = set([])
        self.devs_not_found = set([])
        for dev in devs:
            if os.path.exists(dev):
                self.devs.add(dev)
            else:
                self.devs_not_found.add(dev)

        resDg.Dg.__init__(self, rid=rid, name="raw",
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags,
                          monitor=monitor)

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

