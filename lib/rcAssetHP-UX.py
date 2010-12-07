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
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
from subprocess import *

class Asset(object):
    def __init__(self):
        # echo "selclass qualifier memory;info;wait;infolog"|cstm
        process = Popen(['cstm'], stdin=PIPE, stdout=PIPE, stderr=None)
        (out, err) = process.communicate(input='selclass qualifier memory;info;wait;infolog')
        if process.returncode != 0:
            self.memory = []
        else:
            self.memory = out.split('\n')

        (out, err, ret) = justcall(['print_manifest'])
        if ret != 0:
            self.manifest = []
        else:
            self.manifest = out.split('\n')

    def get_mem_bytes(self):
        cmd = ['swapinfo', '-Mq']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        return str(int(out)//1024)

    def get_mem_banks(self):
        if len(self.memory) == 0:
            return '0'
        banks =  0
        for l in self.memory:
            if 'DIMM ' in l:
                e = l.split()
                if len(e) < 4:
                    continue
                if '--' not in e[4]:
                    banks += 1
        return str(banks-1)

    def get_mem_slots(self):
        if len(self.memory) == 0:
            return '0'
        slots =  0
        for l in self.memory:
            if 'DIMM ' in l:
                slots += 1
        return str(slots-1)

    def get_os_vendor(self):
        return 'HP'

    def get_os_release(self):
        (out, err, ret) = justcall(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out

    def get_os_kernel(self):
        (out, err, ret) = justcall(['swlist', '-l', 'bundle', 'QPKBASE'])
        if ret != 0:
            return 'Unknown'
        for line in out.split('\n'):
            if 'QPKBASE' in line:
                return line.split()[1]
        return 'Unknown'

    def get_os_arch(self):
        (out, err, ret) = justcall(['uname', '-m'])
        if ret != 0:
            return 'Unknown'
        return out

    def get_cpu_freq(self):
        # Processors:         8
        #   4 Intel(R) Itanium 2 9100 series processors (1.59 GHz, 18 MB)
        #      532 MT/s bus, CPU version A1
        #      8 logical processors (2 per socket)
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                e = line.split()
                for i, w in enumerate(e):
                    if 'Hz' in w:
                        break
                return e[i-1].replace('(','')+' '+e[i].replace(',','')
        return 'Unknown'

    def get_cpu_cores(self):
        for line in self.manifest:
            if 'Processors:' in line:
                return line.split()[-1]
        return '0'

    def get_cpu_dies(self):
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                return line.split()[0]
        return '0'

    def get_cpu_model(self):
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                e = line.split()
                return ' '.join(e[1:]).replace('processors','').replace('processor','')
        return 'Unknown'

    def get_serial(self):
        (out, err, ret) = justcall(['getconf', 'MACHINE_SERIAL'])
        if ret != 0:
            return 'Unknown'
        return out

    def get_model(self):
        (out, err, ret) = justcall(['getconf', 'MACHINE_MODEL'])
        if ret != 0:
            return 'Unknown'
        return out

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
