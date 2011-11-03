#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
from rcUtilities import call, which
from rcGlobalEnv import rcEnv
import rcAssetLinux

class Asset(rcAssetLinux.Asset):
    def _get_mem_bytes(self):
        cmd = ['sysctl', 'hw.realmem']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        if len(lines) < 1:
            return '0'
        line = lines[0].split()
        if len(line) < 2:
            return '0'
        mb = int(line[-1])
        return str(mb/1024/1024)

    def _get_os_vendor(self):
        return 'FreeBSD'

    def _get_os_release(self):
        return self._get_os_kernel()

    def _get_os_arch(self):
        cmd = ['sysctl', 'hw.machine_arch']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return line[-1]

    def _get_cpu_model(self):
        cmd = ['sysctl', 'hw.model']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return " ".join(line[1:])

    def _get_cpu_cores(self):
        cmd = ['sysctl', 'hw.ncpu']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return line[-1]
