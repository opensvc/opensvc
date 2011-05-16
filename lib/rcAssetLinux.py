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

    def get_mem_bytes(self):
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

    def get_mem_banks(self):
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

    def get_mem_slots(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Number Of Devices:' in l:
                return l.split()[-1]
        return '0'

    def get_os_vendor(self):
        if os.path.exists('/etc/lsb-release'):
            with open('/etc/lsb-release') as f:
                for line in f.readlines():
                    if 'DISTRIB_ID' in line:
                        return line.split('=')[-1].replace('\n','').strip('"')
        if os.path.exists('/etc/debian_version'):
            return 'Debian'
        if os.path.exists('/etc/SuSE-release'):
            return 'SuSE'
        if os.path.exists('/etc/redhat-release'):
            with open('/etc/redhat-release', 'r') as f:
                if 'CentOS' in f.read():
                    return 'CentOS'
                else:
                    return 'Redhat'
        return 'Unknown'

    def get_os_release(self):
        files = ['/etc/debian_version',
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
                        r = r.replace(self.get_os_vendor(), '').strip()
                        return r
        for f in files:
            if os.path.exists(f):
                (ret, out, err) = call(['cat', f])
                if ret != 0:
                    return 'Unknown'
                return out.split('\n')[0].replace('CentOS','').strip()
        return 'Unknown'

    def get_os_kernel(self):
        (ret, out, err) = call(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_os_arch(self):
        if which('arch') is not None:
            cmd = ['arch']
        else:
            cmd = ['uname', '-m']
        (ret, out, err) = call(cmd)
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def get_cpu_freq(self):
        p = '/proc/cpuinfo'
        if not os.path.exists(p):
            return 'Unknown'
        with open(p, 'r') as f:
            for line in f.readlines():
                if 'cpu MHz' in line:
                    return line.split(':')[1].strip().split('.')[0]
        return 'Unknown'

    def get_cpu_cores(self):
        with open('/proc/cpuinfo') as f:
            lines = f.readlines()
            lines = [l for l in lines if 'core id' in l]
            if len(lines) == 0:
                return '1'
            c = lines[-1].split(':')[-1].replace('\n','').strip()
            c = int(c) + 1
            return str(c)
        return '0'

    def get_cpu_dies(self):
        if self.container:
            return 'n/a'
        c = 0
        for l in self.dmidecode:
            if 'Processor Information' in l:
                c += 1
        return str(c)

    def get_cpu_model(self):
        (ret, out, err) = call(['grep', 'model name', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def get_serial(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Serial Number:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def get_model(self):
        if self.container:
            return 'container'
        for l in self.dmidecode:
            if 'Product Name:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

