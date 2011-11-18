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
import rcAsset

def is_container():
    p = '/proc/1/cgroup'
    if not os.path.exists(p):
        return False
    with open(p, 'r') as f:
        lines = f.readlines()
        if len(lines) != 1:
            return False
        l = lines[0].split(':')
        if l[-1].strip('\n') != '/':
            return True
    return False

class Asset(rcAsset.Asset):
    def __init__(self, node):
        rcAsset.Asset.__init__(self, node)
        self.container = is_container()
        if self.container:
            self.dmidecode = []
        else:
            (ret, out, err) = call(['dmidecode'])
            if ret != 0:
                self.dmidecode = []
            else:
                self.dmidecode = out.split('\n')

    def _get_mem_bytes_esx(self):
        cmd = ['vmware-cmd', '-s', 'getresource', 'system.mem.totalMem']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return '0'
        l = out.split(' = ')
        if len(l) < 2:
            return '0'
        try:
            size = str(int(l[-1])/1024)
        except:
            size = '0'
        return size

    def _get_mem_bytes_hv(self):
        cmd = ['virsh', 'nodeinfo']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        for line in lines:
            if 'Memory size' not in line:
                continue
            l = line.split()
            if len(l) < 2:
                continue
            return l[-2]
        return '0'

    def _get_mem_bytes_phy(self):
        cmd = ['free', '-m']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        if len(lines) < 2:
            return '0'
        line = lines[1].split()
        if len(line) < 2:
            return '0'
        return line[1]

    def is_xen_hv(self):
        c = os.path.join(os.sep, 'proc', 'capabilities')
        if not os.path.exists(c):
            return False
        with open(c, 'r') as f:
            if 'control_d' in f.read():
                return True
        return False

    def is_esx_hv(self):
        return which('vmware-cmd')

    def _get_mem_bytes(self):
        if self.is_xen_hv():
            return self._get_mem_bytes_hv()
        elif self.is_esx_hv():
            return self._get_mem_bytes_esx()
        else:
            return self._get_mem_bytes_phy()

    def _get_mem_banks(self):
        if self.container:
            return 'n/a'
        banks =  0
        inBlock = False
        for l in self.dmidecode:
            if not inBlock and l == "Memory Device":
                 inBlock = True
            if inBlock and "Size:" in l:
                 e = l.split()
                 if len(e) == 3:
                     try:
                         size = int(e[1])
                         banks += 1
                     except:
                         pass
        return str(banks)

    def _get_mem_slots(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Number Of Devices:' in l:
                return l.split()[-1]
        return '0'

    def _get_os_vendor(self):
        if os.path.exists('/etc/lsb-release'):
            with open('/etc/lsb-release') as f:
                for line in f.readlines():
                    if 'DISTRIB_ID' in line:
                        return line.split('=')[-1].replace('\n','').strip('"')
        if os.path.exists('/etc/debian_version'):
            return 'Debian'
        if os.path.exists('/etc/SuSE-release'):
            return 'SuSE'
        if os.path.exists('/etc/vmware-release'):
            return 'VMware'
        if os.path.exists('/etc/redhat-release'):
            with open('/etc/redhat-release', 'r') as f:
                buff = f.read()
                if 'CentOS' in buff:
                    return 'CentOS'
                elif 'Oracle' in buff:
                    return 'Oracle'
                else:
                    return 'Redhat'
        return 'Unknown'

    def _get_os_release(self):
        files = ['/etc/debian_version',
                 '/etc/vmware-release',
                 '/etc/redhat-release']
        if os.path.exists('/etc/SuSE-release'):
            v = []
            with open('/etc/SuSE-release') as f:
                for line in f.readlines():
                    if 'VERSION' in line:
                        v += [line.split('=')[-1].replace('\n','').strip('" ')]
                    if 'PATCHLEVEL' in line:
                        v += [line.split('=')[-1].replace('\n','').strip('" ')]
	    return '.'.join(v)
        if os.path.exists('/etc/lsb-release'):
            with open('/etc/lsb-release') as f:
                for line in f.readlines():
                    if 'DISTRIB_DESCRIPTION' in line:
                        r = line.split('=')[-1].replace('\n','').strip('"')
                        r = r.replace(self._get_os_vendor(), '').strip()
                        return r
        for f in files:
            if os.path.exists(f):
                (ret, out, err) = call(['cat', f])
                if ret != 0:
                    return 'Unknown'
                return out.split('\n')[0].replace(self._get_os_vendor(), '').strip()
        return 'Unknown'

    def _get_os_kernel(self):
        (ret, out, err) = call(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_os_arch(self):
        if which('arch') is not None:
            cmd = ['arch']
        else:
            cmd = ['uname', '-m']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_cpu_freq(self):
        p = '/proc/cpuinfo'
        if not os.path.exists(p):
            return 'Unknown'
        with open(p, 'r') as f:
            for line in f.readlines():
                if 'cpu MHz' in line:
                    return line.split(':')[1].strip().split('.')[0]
        return 'Unknown'

    def _get_cpu_cores(self):
        if self.is_esx_hv():
            return '0'
        with open('/proc/cpuinfo') as f:
            lines = f.readlines()
            lines = [l for l in lines if 'core id' in l]
            if len(lines) == 0:
                return self._get_cpu_dies()
            c = lines[-1].split(':')[-1].replace('\n','').strip()
            c = int(c) + 1
            return str(c)
        return '0'

    def _get_cpu_dies_dmi(self):
        if self.container:
            return 'n/a'
        n = 0
        for l in self.dmidecode:
            if 'Processor Information' in l:
                n += 1
        return str(n)

    def _get_cpu_dies_cpuinfo(self):
        if self.container:
            return 'n/a'
        c = 0
        for l in self.dmidecode:
            if 'Processor Information' in l:
                c += 1
        return str(c)

    def _get_cpu_dies(self):
        if self.is_esx_hv():
            return self._get_cpu_dies_dmi()
        return self._get_cpu_dies_cpuinfo()

    def _get_cpu_model(self):
        (ret, out, err) = call(['grep', 'model name', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def _get_serial(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Serial Number:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def _get_model(self):
        if self.container:
            return 'container'
        for l in self.dmidecode:
            if 'Product Name:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

