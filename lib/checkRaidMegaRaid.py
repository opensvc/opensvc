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

    def find_megacli(self):
        if which(self.megacli):
            return self.megacli
        for prefix in self.prefixes:
            megacli = os.path.join(prefix, self.megacli)
            if os.path.exists(megacli):
                return megacli
        return

    def do_check(self):
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
                if slot != "" and errs > 0:
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
        if slot != "" and errs > 0:
            r.append({
                 'chk_instance': slot,
                  'chk_value': str(errs),
                  'chk_svcname': '',
                 })
        return r
