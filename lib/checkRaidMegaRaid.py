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
import os
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv

class check(checks.check):
    prefixes = [os.path.join(os.sep, "usr", "local", "admin")]
    megacli = "MegaCli"
    chk_type = "raid"
    chk_name = "MegaCli"

    def find_megacli(self):
        for prog in self.megacli:
            if which(prog):
                return prog
            for prefix in self.prefixes:
                megacli = os.path.join(prefix, prog)
                if os.path.exists(megacli):
                    return megacli
        return

    def do_check(self):
        r = self.do_check_ldpdinfo()
        r += self.do_check_bbustatus()
        return r

    def do_check_ldpdinfo(self):
        megacli = self.find_megacli()
        if megacli is None:
            return self.undef
        os.chdir(rcEnv.pathtmp)
        logs = [os.path.join(rcEnv.pathtmp, 'MegaSAS.log'),
                os.path.join(rcEnv.pathtmp, 'MegaCli.log'),
                os.path.join(rcEnv.pathtmp, 'MegaRaid.log')]
        for log in logs:
            try:
                os.unlink(log)
            except OSError, e:
                if e.errno == 2:
                    pass
                else:
                    raise
        cmd = [megacli, '-LdPdInfo', '-aALL']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        slot = ""
        errs = 0
        for line in lines:
            if line.startswith('Adapter'):
                l = line.split('#')
                if slot != "":
                    r.append({
                             'chk_instance': slot,
                              'chk_value': str(errs),
                              'chk_svcname': '',
                             })
                slot = 'slot'+l[-1]
                errs = 0
            if (line.startswith('State:') and 'Optimal' not in line) or \
               (line.startswith('Firmware state:') and 'Online' not in line):
                errs += 1
        if slot != "":
            r.append({
                 'chk_instance': slot,
                  'chk_value': str(errs),
                  'chk_svcname': '',
                 })
        return r

    def do_check_bbustatus(self):
        megacli = self.find_megacli()
        if megacli is None:
            return self.undef
        os.chdir(rcEnv.pathtmp)
        logs = [os.path.join(rcEnv.pathtmp, 'MegaSAS.log'),
                os.path.join(rcEnv.pathtmp, 'MegaCli.log'),
                os.path.join(rcEnv.pathtmp, 'MegaRaid.log')]
        for log in logs:
            try:
                os.unlink(log)
            except OSError, e:
                if e.errno == 2:
                    pass
                else:
                    raise
        cmd = [megacli, '-AdpBbuCmd', '-GetBbuStatus', '-aALL']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) == 0:
            return self.undef
        r = []
        slot = ""
        for line in lines:
            if 'Adapter:' in line:
                l = line.split()
                slot = 'slot'+l[-1]
            if line.startswith('BatteryType:') and 'No Battery' in line:
                val = 1
                r.append({
                          'chk_instance': '%s battery NoBattery'%slot,
                          'chk_value': str(val),
                          'chk_svcname': '',
                         })
            if line.startswith('Relative State of Charge:'):
                val = line.strip('%').split()[-1]
                r.append({
                          'chk_instance': '%s battery charge'%slot,
                          'chk_value': str(val),
                          'chk_svcname': '',
                         })
            if line.startswith('Temperature:'):
                val = line.split()[-2]
                r.append({
                          'chk_instance': '%s battery temp'%slot,
                          'chk_value': str(val),
                          'chk_svcname': '',
                         })
            if line.startswith('isSOHGood:'):
                if 'Yes' in line:
                    val = 0
                else:
                    val = 1
                r.append({
                          'chk_instance': '%s battery isSOHGood'%slot,
                          'chk_value': str(val),
                          'chk_svcname': '',
                         })
        return r
