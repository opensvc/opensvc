#!/usr/bin/python2.6
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

class Asset(object):
    def __init__(self):
        self.sphw = {}
        (ret, out) = call(['system_profiler', 'SPHardwareDataType'])
        if ret == 0:
            for line in out.split('\n'):
                l = line.split(':')
                if len(l) != 2: continue
                self.sphw[l[0].strip()] = l[1].strip()

    def get_mem_bytes(self):
        if 'Memory' not in self.sphw:
            return '0'
        m = self.sphw['Memory'].split()
        size = int(m[0])
        unit = m[1]
        if unit == 'GB':
            size = size * 1024
        elif unit == 'MB':
            pass
        else:
            raise
        return str(size)

    def get_mem_banks(self):
        return '0'

    def get_mem_slots(self):
        return '0'

    def get_os_vendor(self):
        return 'Apple'

    def get_os_release(self):
        (ret, out) = call(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split()[0]

    def get_os_kernel(self):
        return self.get_os_release()

    def get_os_arch(self):
        cmd = ['uname', '-m']
        (ret, out) = call(cmd)
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_cpu_freq(self):
        if 'Processor Speed' not in self.sphw:
            return '0'
        return self.sphw['Processor Speed']

    def get_cpu_cores(self):
        if 'Total Number of Cores' not in self.sphw:
            return '0'
        return self.sphw['Total Number of Cores']

    def get_cpu_dies(self):
        if 'Number of Processors' not in self.sphw:
            return '0'
        return self.sphw['Number of Processors']

    def get_cpu_model(self):
        if 'Processor Name' not in self.sphw:
            return '0'
        return self.sphw['Processor Name']

    def get_serial(self):
        if 'Hardware UUID' not in self.sphw:
            return '0'
        return self.sphw['Hardware UUID']

    def get_model(self):
        if 'Model Name' not in self.sphw:
            return '0'
        return self.sphw['Model Name']

    def get_environnement(self):
        f = os.path.join(rcEnv.pathvar, 'host_mode')
        if os.path.exists(f):
            (ret, out) = call(['cat', f])
            if ret == 0:
                return out.split('\n')[0]
        return 'Unknown'

    def get_asset_dict(self):
        d = {}
        d['nodename'] = rcEnv.nodename
        d['os_name'] = rcEnv.sysname
        d['os_vendor'] = self.get_os_vendor()
        d['os_release'] = self.get_os_release()
        d['os_kernel'] = self.get_os_kernel()
        d['os_arch'] = self.get_os_arch()
        d['mem_bytes'] = self.get_mem_bytes()
        d['mem_banks'] = self.get_mem_banks()
        d['mem_slots'] = self.get_mem_slots()
        d['cpu_freq'] = self.get_cpu_freq()
        d['cpu_cores'] = self.get_cpu_cores()
        d['cpu_dies'] = self.get_cpu_dies()
        d['cpu_model'] = self.get_cpu_model()
        d['serial'] = self.get_serial()
        d['model'] = self.get_model()
        d['environnement'] = self.get_environnement()
        return d
