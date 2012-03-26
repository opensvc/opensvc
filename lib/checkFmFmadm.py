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
    prefixes = [os.path.join(os.sep, "usr", "sbin")]
    fmadm = "fmadm"
    chk_type = "fm"
    chk_name = "Solaris fmadm"

    def find_fmadm(self):
        if which(self.fmadm):
            return self.fmadm
        for prefix in self.prefixes:
            fmadm = os.path.join(prefix, self.fmadm)
            if os.path.exists(fmadm):
                return fmadm
        return

    def do_check(self):
        r = self.do_check_ldpdinfo()
        return r

    def do_check_ldpdinfo(self):
        fmadm = self.find_fmadm()
        if fmadm is None:
            return self.undef
        os.chdir(rcEnv.pathtmp)
        cmd = [fmadm, 'faulty']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.undef
        r = []
        r.append({
              'chk_instance': 'faults ',
              'chk_value': str(len(out)),
              'chk_svcname': '',
            })
        return r
