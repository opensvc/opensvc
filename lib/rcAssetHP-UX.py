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
import rcAsset
import datetime

os.environ['PATH'] += ":/opt/ignite/bin:/opt/propplus/bin"

class Asset(rcAsset.Asset):
    def __init__(self, node):
        rcAsset.Asset.__init__(self, node)

        out, err, ret = justcall(['print_manifest'])
        if ret != 0:
            self.manifest = []
        else:
            self.manifest = out.split('\n')

        self.parse_memory()

    def _get_mem_bytes(self):
        cmd = ['swapinfo', '-Mq']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        return str(int(out)//1024)

    def parse_memory(self):
        self.banks =  0
        self.slots =  0

        cmd = ['cprop', '-summary', '-c', 'Memory']
        out, err, ret = justcall(cmd)

        if ret != 0:
            return '0'

        in_banks = True

        for line in out.split('\n'):
            if 'Empty Slots' in line:
                in_banks = False
            elif 'Instance' in line:
                self.slots += 1
                if in_banks:
                    self.banks += 1

    def _get_mem_banks(self):
        return str(self.banks)

    def _get_mem_slots(self):
        return str(self.slots)

    def _get_os_vendor(self):
        return 'HP'

    def _get_os_release(self):
        (out, err, ret) = justcall(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0].strip()

    def _get_os_kernel(self):
        (out, err, ret) = justcall(['swlist', '-l', 'bundle', 'QPKBASE'])
        if ret != 0:
            return 'Unknown'
        for line in out.split('\n'):
            if 'QPKBASE' in line:
                return line.split()[1]
        return 'Unknown'

    def _get_os_arch(self):
        (out, err, ret) = justcall(['uname', '-m'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0].strip()

    def _get_cpu_freq(self):
        freq = self._get_cpu_freq_manifest()
        if freq == "Unknown":
            freq = self._get_cpu_freq_adb()
        return freq

    def _get_cpu_freq_manifest(self):
        m = self._get_cpu_model()
        if '(' not in m:
            return "Unknown"
        s = m.split('(')[-1]
        s = s.split(',')[0]
        freq, unit = s.split()
        if unit == 'GHz':
            try:
                freq = float(freq)
            except:
                return "Unknown"
            freq = str(int(freq * 1000))
        return freq

    def _get_cpu_freq_adb(self):
        process = Popen(['adb', '/stand/vmunix', '/dev/kmem'], stdin=PIPE, stdout=PIPE, stderr=None)
        (out, err) = process.communicate(input='itick_per_usec/2d')
        if process.returncode != 0:
            process = Popen(['adb', '-k', '/stand/vmunix', '/dev/mem'], stdin=PIPE, stdout=PIPE, stderr=None)
            (out, err) = process.communicate(input='itick_per_usec/D')
        if process.returncode != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 2:
            return 'Unknown'
        return lines[1].split()[-1]

    def _get_cpu_cores(self):
        for line in self.manifest:
            if 'Processors:' in line:
                return line.split()[-1]
        return '0'

    def _get_cpu_dies(self):
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                return line.split()[0]
        return '0'

    def _get_cpu_model(self):
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                e = line.split()
                return ' '.join(e[1:]).replace('processors','').replace('processor','')
        return 'Unknown'

    def _get_serial(self):
        (out, err, ret) = justcall(['getconf', 'MACHINE_SERIAL'])
        if ret != 0:
            return 'Unknown'
        return out.strip()

    def _get_model(self):
        (out, err, ret) = justcall(['getconf', 'MACHINE_MODEL'])
        if ret != 0:
            return 'Unknown'
        return out.strip()

    def __get_hba(self):
        if hasattr(self, "hba"):
            return self.hba
        self.hba = []
        cmd = ['/usr/sbin/ioscan', '-FunC', 'fc']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.hba
        lines = out.split('\n')
        if len(lines) < 2:
            return self.hba
        for line in lines:
            if '/dev/' not in line:
                continue
            dev = line.strip()
            hba_type = 'fc'

            cmd = ['/opt/fcms/bin/fcmsutil', dev]
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            for _line in out.split('\n'):
                if not 'N_Port Port World Wide Name' in _line:
                    continue
                hba_id = _line.split('=')[-1].strip().lstrip("0x")

            cmd = ['/opt/fcms/bin/fcmsutil', dev, 'get', 'remote', 'all']
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            targets = []
            for _line in out.split('\n'):
                if not 'Target Port World Wide Name' in _line:
                    continue
                targets.append(_line.split('=')[-1].strip().lstrip("0x"))

            self.hba.append((hba_id, hba_type, targets))
        return self.hba

    def _get_hba(self):
        hba = self.__get_hba()
        l = []
        for hba_id, hba_type, targets in hba:
            l.append((hba_id, hba_type))
        return l

    def _get_targets(self):
        hba = self.__get_hba()
        l = []
        for hba_id, hba_type, targets in hba:
            for target in targets:
                l.append((hba_id, target))
        return l

