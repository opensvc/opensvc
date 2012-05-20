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
import checks
from rcUtilities import justcall, which
import os
from rcGlobalEnv import rcEnv
import datetime

class check(checks.check):
    chk_type = "mcelog"
    mcelog_p = "/var/log/mcelog"
    marker_p = os.path.join(rcEnv.pathtmp, "checkMceLinunx.marker")

    def get_last_marker(self):
        try:
            f = open(self.marker_p, 'r')
        except:
            return None
        buff = f.read()
        f.close()
        return buff

    def gen_marker(self):
        return "opensvc marker " + str(datetime.datetime.now())+'\n'

    def update_marker(self):
        m = self.gen_marker()
        try:
            f = open(self.mcelog_p, "a")
            f.write(m)
            f.close()
        except:
            return
        try:
            f = open(self.marker_p, "w")
            f.write(m)
            f.close()
        except:
            return

    def do_check(self):
        if not os.path.exists(self.mcelog_p):
            return self.undef

        if not which("mcelog"):
            return self.undef

        try:
            f = open(self.mcelog_p, "r")
        except:
            return self.undef

        marker = self.get_last_marker()
        marker_found = False
        l = 0
        total = 0

        for line in f.readlines():
            total += 1
            if line == marker:
                marker_found = True
                continue
            if not marker_found:
                continue
            l += 1

        if not marker_found:
            l = total

        r = []
        r.append({
                  'chk_instance': "new lines",
                  'chk_value': str(l),
                  'chk_svcname': "",
                 })

        self.update_marker()
        return r
