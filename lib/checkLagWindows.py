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
import checks
import os
import wmi

class check(checks.check):
    chk_type = "lag"
    chk_name = "Windows network link aggregate"

    def do_check(self):
        self.w = wmi.WMI(namespace="root\hpq")
        r = []
        for team in self.w.HP_EthernetTeam():
            r += self.do_check_team(team)
        return r

    def do_check_team(self, team):
        r = []
        inst = team.Description
        val = team.RedundancyStatus
        r.append({
                'chk_instance': inst+'.redundancy',
                'chk_value': str(val),
                'chk_svcname': '',
               })
        return r

if __name__ == "__main__":
    o = check()
    o.do_check()
