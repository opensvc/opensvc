#
# Copyright (c) 2012 Lucien Hercaud <hercaud@hercaud.com>
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
    sas2ircu = "sas2ircu"
    chk_type = "raid"
    chk_name = "LSI SAS200"

    def find_sas2ircu(self):
        if which(self.sas2ircu):
            return self.sas2ircu
        for prefix in self.prefixes:
            sas2ircu = os.path.join(prefix, self.sas2ircu)
            if os.path.exists(sas2ircu):
                return sas2ircu
        return

    def do_check(self):
        r = self.do_check_ldpdinfo()
        return r

    def do_check_ldpdinfo(self):
        sas2ircu = self.find_sas2ircu()
        if sas2ircu is None:
            return self.undef
        os.chdir(rcEnv.pathtmp)
        logs = [os.path.join(rcEnv.pathtmp, 'sas2ircu.log')]
        for log in logs:
            try:
                os.unlink(log)
            except OSError as e:
                if e.errno == 2:
                    pass
                else:
                    raise
        cmd = [sas2ircu, 'LIST']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        idx = []
        lines = out.split('\n')
        for line in lines:
            if 'SAS20' in line:
                l = line.split()
                idx.append(l[0])

        r = []
        errs = 0
        for ix in idx:
            cmd = [sas2ircu, str(ix), 'DISPLAY']
            out, err, ret = justcall(cmd)
            lines = out.split('\n')
            ctrl = "ctrl:"+str(ix)
            slot=""
            chk_dsk = 0
            for line in lines:
                if line.startswith('IR volume'):
                    chk_dsk = 2
                if line.startswith('  Volume Name') and 'Virtual Disk' in line and (chk_dsk == 2):
                    l = line.split()
                    slot = 'LD'+str(l[-1])
                if line.startswith('  Status of volume') and (chk_dsk == 2):
                    if 'Okay (OKY)' not in line:
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '1', 'chk_svcname': '', })
                        errs += 1
                    else :
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '0', 'chk_svcname': '', })
                if line.startswith('Device is a Hard disk'):
                    chk_dsk = 1
                if line.startswith('  Enclosure #') and (chk_dsk == 1):
                    l = line.split()
                    enc = l[-1]
                if line.startswith('  Slot #') and (chk_dsk == 1):
                    l = line.split()
                    slot = 'PD'+str(enc)+':'+str(l[-1])
                if line.startswith('  State') and (chk_dsk == 1):
                    if 'Optimal (OPT)' not in line:
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '1', 'chk_svcname': '', })
                        errs += 1
                    else :
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '0', 'chk_svcname': '', })
                if line.startswith('Device is a Enclosure services device'):
                    chk_dsk = 3
                if line.startswith('  Enclosure #') and (chk_dsk == 3):
                    l = line.split()
                    slot = 'Enc'+str(l[-1])
                if line.startswith('  State') and (chk_dsk == 3):
                    if 'Standby (SBY)' not in line:
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '1', 'chk_svcname': '', })
                        errs += 1
                    else :
                        r.append({ 'chk_instance': ctrl+','+slot, 'chk_value': '0', 'chk_svcname': '', })
            r.append({ 'chk_instance': 'all SAS20*', 'chk_value': str(errs), 'chk_svcname': '', })
        return r
