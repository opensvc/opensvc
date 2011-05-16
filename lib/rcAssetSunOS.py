#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import rcAsset

class Asset(rcAsset.Asset):
    def __init__(self, node):
        rcAsset.Asset.__init__(self, node)
        (ret, out, err) = call(['prtconf'])
        if ret != 0:
            self.prtconf = []
        else:
            self.prtconf = out.split('\n')

    def get_mem_bytes(self):
        for l in self.prtconf:
            if 'Memory size:' in l:
                return l.split(':')[-1].split()[0]
        return '0'

    def get_mem_banks(self):
        return '0'

    def get_mem_slots(self):
        return '0'

    def get_os_vendor(self):
        return 'Oracle'

    def get_os_name(self):
        f = '/etc/release'
        if os.path.exists(f):
            (ret, out, err) = call(['cat', f])
            if ret != 0:
                return 'Unknown'
            if 'OpenSolaris' in out:
                return 'OpenSolaris'
        return 'SunOS'

    def get_os_release(self):
        f = '/etc/release'
        if os.path.exists(f):
            (ret, out, err) = call(['cat', f])
            if ret != 0:
                return 'Unknown'
            return out.split('\n')[0].replace('OpenSolaris','').strip()
        return 'Unknown'

    def get_os_kernel(self):
        (ret, out, err) = call(['uname', '-v'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_os_arch(self):
        (ret, out, err) = call(['uname', '-m'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_cpu_freq(self):
        (ret, out, err) = call(['psrinfo', '-pv'])
        if ret != 0:
            return '0'
        for w in out.split():
            if 'MHz)' in w:
                return ' '.join([prev, w.strip(')')])
            prev = w
        return '0'

    def get_cpu_cores(self):
        (ret, out, err) = call(['psrinfo'])
        if ret != 0:
            return '0'
        return str(len(out.split('\n'))-1)

    def get_cpu_dies(self):
        (ret, out, err) = call(['psrinfo', '-p'])
        if ret != 0:
            return '0'
        return out.split('\n')[0]

    def get_cpu_model(self):
        (ret, out, err) = call(['psrinfo', '-pv'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 3:
            return 'Unknown'
        return lines[2].strip()

    def get_serial(self):
        (ret, out, err) = call(['hostid'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_model(self):
        for l in self.prtconf:
            if 'System Configuration:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

