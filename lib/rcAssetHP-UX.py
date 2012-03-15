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

class Asset(rcAsset.Asset):
    def __init__(self, node):
        rcAsset.Asset.__init__(self, node)
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

        self.parse_memory()

    def _get_mem_bytes(self):
        cmd = ['swapinfo', '-Mq']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        return str(int(out)//1024)

    def parse_memory(self):
        """
	   DIMM Slot      Size (MB)
	   ---------      ---------
		  0A           2048
		  3D              0
	   ---------      ---------

        or

	   DIMM Location          Size(MB)     DIMM Location          Size(MB)
	   --------------------   --------     --------------------   --------
	   Ext 0 DIMM 0A          2048         Ext 0 DIMM 0B          2048    
	   Ext 0 DIMM 5C          ----         Ext 0 DIMM 5D          ----    

	   Ext 0 Total: 32768 (MB)
        """
        if len(self.memory) == 0:
            return '0'
        self.banks =  0
        self.slots =  0
        begin = 0
        end = 0
        for i, l in enumerate(self.memory):
            if 'DIMM ' in l:
                begin = i+2
        if begin == 0:
            return '0'
        for i in range(begin, len(self.memory)):
            e = self.memory[i].split()
            if len(self.memory[i]) == 0 or '--' in e[0]:
                end = i
                break
        if end == 0:
            return '0'
        for i in range(begin, end):
            e = self.memory[i].split()
            n = len(e)
            if n == 2:
                # old format
                self.slots += 1
                if e[1] != '0':
                    self.banks += 1
                continue
            if n >= 5:
                # new format 1st col
                self.slots += 1
                if not '--' in e[4]:
                    self.banks += 1
            if n == 10:
                # new format 2nd col
                self.slots += 1
                if not '--' in e[9]:
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
        process = Popen(['adb', '/stand/vmunix', '/dev/kmem'], stdin=PIPE, stdout=PIPE, stderr=None)
        (out, err) = process.communicate(input='itick_per_usec/2d')
        if process.returncode != 0:
            process = Popen(['adb', '-k', '/stand/vmunix', '/dev/mem'], stdin=PIPE, stdout=PIPE, stderr=None)
            (out, err) = process.communicate(input='itick_per_usec/D')
        if process.returncode != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) != 2:
            return 'Unknown'
        return lines[1].strip()

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
        return out

    def _get_model(self):
        (out, err, ret) = justcall(['getconf', 'MACHINE_MODEL'])
        if ret != 0:
            return 'Unknown'
        return out

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

