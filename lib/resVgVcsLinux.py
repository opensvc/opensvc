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
Res = __import__("resVgLinux")
import rcStatus
import rcExceptions as ex

class Vg(Res.Vg):

    def start(self):
        pass

    def stop(self):
        pass

    def _status(self, verbose=False):
        try:
            s = self.svc.get_res_val(self.vcs_name, 'State')
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

        if s == "ONLINE":
            return rcStatus.UP
        elif s == "OFFLINE":
            return rcStatus.DOWN
        else:
            self.status_log(s)
            return rcStatus.WARN

