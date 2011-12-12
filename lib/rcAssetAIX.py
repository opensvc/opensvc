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
import os
import datetime
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import rcAsset

class Asset(rcAsset.Asset):
    def __init__(self, node):
        rcAsset.Asset.__init__(self, node)
        (out, err, ret) = justcall(['prtconf'])
        if ret != 0:
            self.prtconf = []
        else:
            self.prtconf = out.split('\n')
        self.lpar = self.is_lpar()

    def is_lpar(self):
        cmd = ["prtconf", "-L"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise
        if '-1' in out:
            return False
        return True

    def _get_mem_bytes(self):
        cmd = ["prtconf", "-m"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return '0'
        l = out.split()
        if 'Memory Size:' not in out:
            return '0'
        if len(l) != 4:
            return '0'

        size = int(l[2])
        unit = l[3]

        if unit == 'GB':
            size = size * 1024
        elif unit == 'MB':
            pass
        else:
            return '0'

        return str(size)

    def _get_mem_banks(self):
        if self.lpar:
            return '0'
        return 'TODO'

    def _get_mem_slots(self):
        if self.lpar:
            return '0'
        return 'TODO'

    def _get_os_vendor(self):
        return 'IBM'

    def _get_os_name(self):
        return 'AIX'

    def _get_os_release(self):
        cmd = ["oslevel", "-s"]
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        return out.strip()

    def _get_os_kernel(self):
        cmd = ["oslevel", "-r"]
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        return out.strip()

    def _get_os_arch(self):
        for line in self.prtconf:
            if "Kernel Type:" in line:
                return line.split(":")[-1].strip()
        return 'Unknown'

    def _get_cpu_freq(self):
        for line in self.prtconf:
            if "Processor Clock Speed:" in line:
                return line.split(":")[-1].split()[0].strip()
        return '0'

    def _get_cpu_cores(self):
        cmd = ["bindprocessor", "-q"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return '0'
        l = out.split(":")
        return str(len(l[-1].strip().split()))

    def _get_cpu_dies(self):
        cmd = ["lsdev", "-Cc", "processor"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return '0'
        return str(len([line for line in out.split('\n') if 'proc' in line]))

    def _get_cpu_model(self):
        for line in self.prtconf:
            if "Processor Type:" in line:
                return line.split(":")[-1].strip()
        return 'Unknown'

    def _get_serial(self):
        for line in self.prtconf:
            if "Machine Serial Number:" in line:
                return line.split(":")[-1].strip()
        return 'Unknown'

    def _get_model(self):
        for line in self.prtconf:
            if "System Model:" in line:
                return line.split(":")[-1].strip()
        return 'Unknown'
