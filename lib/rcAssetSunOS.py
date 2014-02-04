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
from rcUtilities import justcall, which
from rcGlobalEnv import rcEnv
import rcAsset
from rcZone import is_zone

class Asset(rcAsset.Asset):
    def __init__(self, node=None):
        rcAsset.Asset.__init__(self, node)
        self.osver = 0.
        self.zone = is_zone()

        (out, err, ret) = justcall(['prtdiag'])
        if ret != 0 and len(out) < 4:
            self.prtdiag = []
        else:
            self.prtdiag = out.split('\n')
        (out, err, ret) = justcall(['prtconf'])
        if ret != 0 and len(out) < 4:
            self.prtconf = []
        else:
            self.prtconf = out.split('\n')

    def _get_mem_bytes(self):
        for l in self.prtconf:
            if 'Memory size:' in l:
                return l.split(':')[-1].split()[0]
        return '0'

    def _get_mem_banks(self):
        l = [e for e in self.prtdiag if 'DIMM' in e and 'in use' in e]
        return str(len(l))

    def _get_mem_slots(self):
        l = [e for e in self.prtdiag if 'DIMM' in e]
        return str(len(l))

    def _get_os_vendor(self):
        return 'Oracle'

    def _get_os_name(self):
        f = '/etc/release'
        if os.path.exists(f):
            (out, err, ret) = justcall(['cat', f])
            if ret != 0:
                return 'Unknown'
            if 'OpenSolaris' in out:
                return 'OpenSolaris'
        return 'SunOS'

    def _get_os_release(self):
        f = '/etc/release'
        if os.path.exists(f):
            (out, err, ret) = justcall(['cat', f])
            if ret != 0:
                return 'Unknown'
            return out.split('\n')[0].replace('OpenSolaris','').replace('Oracle', '').strip()
        return 'Unknown'

    def _get_os_kernel(self):
        (out, err, ret) = justcall(['uname', '-v'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) == 0:
            return 'Unknown'
        try:
            self.osver = float(lines[0])
        except:
            return lines[0]
        if self.osver < 11.:
            return lines[0]
        else:
            (out, err, ret) = justcall(['pkg', 'info', 'entire'])
            if ret != 0:
                return 'Unknown'
            nfo = out.split('\n')
            for l in nfo:
                if 'Version: ' in l:
                    if 'SRU' in l:
                        return ' '.join([lines[0], 'SRU', l.split()[6].strip(')')])
                    elif lines[0] in l:
                        return l.split()[4].strip(')')
                    else:
                        return ' '.join([lines[0], l.split()[4]])
        return 'Unknown'

    def _get_os_arch(self):
        (out, err, ret) = justcall(['uname', '-m'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_cpu_freq(self):
        (out, err, ret) = justcall(['/usr/sbin/psrinfo', '-pv'])
        if ret != 0:
            return '0'
        for w in out.split():
            if 'MHz)' in w:
                return ' '.join([prev, w.strip(')')])
            prev = w
        (out, err, ret) = justcall(['kstat', 'cpu_info'])
        if ret != 0:
            return '0'
        l = out.split()
        if 'clock_MHz' in l:
            freq = l[l.index('clock_MHz')+1]
            return freq
        return '0'

    def _get_cpu_cores(self):
        cmd = ['kstat', 'cpu_info']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return '0'
        core_ids = set([])
        if "core_id" in out:
            keyword = "core_id"
        else:
            keyword = "chip_id"
        for line in out.split('\n'):
            if not line.strip().startswith(keyword):
                continue
            core_ids.add(line.split()[-1])
        return str(len(core_ids))

    def _get_cpu_threads(self):
        out, err, ret = justcall(['/usr/sbin/psrinfo'])
        if ret != 0:
            return '0'
        return str(len(out.split('\n'))-1)

    def _get_cpu_dies(self):
        (out, err, ret) = justcall(['/usr/sbin/psrinfo', '-p'])
        if ret != 0:
            return '0'
        return out.split('\n')[0]

    def _get_cpu_model(self):
        (out, err, ret) = justcall(['/usr/sbin/psrinfo', '-pv'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        lines = [line for line in lines if len(line) > 0]
        if len(lines) == 0:
            return 'Unknown'
        model = lines[-1].strip()
        if model.startswith('The '):
            model = model.replace('The ', '')
        known_garbage = [' (chipid', ' (portid', ' physical proc']
        for s in known_garbage:
            try:
                i = model.index(s)
                model = model[:i]
            except ValueError:
                continue
        return model

    def _get_serial(self):
        if which("sneep"):
            cmd = ['sneep']
        else:
            cmd = ['hostid']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_model(self):
        if self.zone:
            return "Solaris Zone"
        for l in self.prtdiag:
            if 'System Configuration:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def __get_hba(self):
        # fc / fcoe
        """
        # cfgadm -s match="exact,select=type(fc-fabric)"
        Ap_Id      Type         Receptacle   Occupant     Condition
        c5         fc-fabric    connected    configured   unknown
        """
        l = []
        if not which('cfgadm'):
            return []
        if not which('luxadm'):
            return []
        cmd = ['cfgadm', '-lv', '-s', 'match=exact,select=type(fc-fabric)']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []
        words = out.split()
        hba_names = [word for word in words if word.startswith("/devices/")]

        if len(hba_names) == 0:
            return []

        hba_type = 'fc'
        for hba_name in hba_names:
            targets = []
            cmd = ['luxadm', '-e', 'dump_map', hba_name]
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            lines = out.split('\n')
            if len(lines) < 2:
                continue
            for line in lines[1:]:
                words = line.split()
                if len(words) < 5:
                    continue
                if 'Host Bus' in line:
                    hba_id = words[3]
                else:
                    targets.append(words[3])
            l.append((hba_id, hba_type, targets))

        return l

    def _get_hba(self):
        l = self.__get_hba()
        return map(lambda x: (x[0], x[1]), l)

    def _get_targets(self):
        l = self.__get_hba()
        m = []
        for hba_id, hba_type, targets in l:
             for target in targets:
                 m.append((hba_id, target))
        return m

if __name__ == "__main__":
    print(Asset()._get_cpu_model())
