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
from rcUtilities import justcall, which
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
        self.detect_xen()
        if self.container:
            self.dmidecode = []
        else:
            (out, err, ret) = justcall(['dmidecode'])
            if ret != 0:
                self.dmidecode = []
            else:
                self.dmidecode = out.split('\n')

    def _get_mem_bytes_esx(self):
        cmd = ['vmware-cmd', '-s', 'getresource', 'system.mem.totalMem']
        (out, err, ret) = justcall(cmd)
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
        if which('virsh'):
            return self._get_mem_bytes_virsh()
        if which('xm'):
            return self._get_mem_bytes_xm()
        else:
            return '0'

    def _get_mem_bytes_virsh(self):
        cmd = ['virsh', 'nodeinfo']
        (out, err, ret) = justcall(cmd)
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

    def _get_mem_bytes_xm(self):
        cmd = ['xm', 'info']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        for line in lines:
            if 'total_mem' not in line:
                continue
            l = line.split(':')
            if len(l) < 2:
                continue
            return l[-1]
        return '0'

    def _get_mem_bytes_phy(self):
        cmd = ['free', '-m']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        if len(lines) < 2:
            return '0'
        line = lines[1].split()
        if len(line) < 2:
            return '0'
        return line[1]

    def detect_xen(self):
        c = os.path.join(os.sep, 'proc', 'xen', 'capabilities')
        self.xenguest = False
        self.xenhv = False
        if not os.path.exists(c):
            return
        with open(c, 'r') as f:
            if 'control_d' in f.read():
                self.xenhv = True
            else:
                self.xenguest = True

    def is_esx_hv(self):
        return which('vmware-cmd')

    def _get_mem_bytes(self):
        if self.xenhv:
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
        if os.path.exists('/etc/oracle-release'):
            return 'Oracle'
        if os.path.exists('/etc/redhat-release'):
            with open('/etc/redhat-release', 'r') as f:
                buff = f.read()
                if 'CentOS' in buff:
                    return 'CentOS'
                elif 'Oracle' in buff:
                    return 'Oracle'
                else:
                    return 'Red Hat'
        return 'Unknown'

    def _get_os_release(self):
        files = ['/etc/debian_version',
                 '/etc/vmware-release',
                 '/etc/oracle-release',
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
        if os.path.exists('/etc/oracle-release') and \
           os.path.exists('/etc/redhat-release'):
            with open('/etc/oracle-release') as f1:
                if " VM " in f1.read():
                    with open('/etc/redhat-release') as f2:
                        return f2.read().split('\n')[0].replace(self._get_os_vendor(), '').strip()
        for f in files:
            if os.path.exists(f):
                (out, err, ret) = justcall(['cat', f])
                if ret != 0:
                    return 'Unknown'
                return out.split('\n')[0].replace(self._get_os_vendor(), '').strip()
        return 'Unknown'

    def _get_os_kernel(self):
        (out, err, ret) = justcall(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_os_arch(self):
        if which('arch') is not None:
            cmd = ['arch']
        else:
            cmd = ['uname', '-m']
        (out, err, ret) = justcall(cmd)
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
            c = len(lines)
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
	(out, err, ret) = justcall(['grep', 'processor', '/proc/cpuinfo'])
	if ret != 0:
	    return '1'
	lines = out.split('\n')
	c = lines[-2].split(':')[-1].replace('\n','').strip()
	c = int(c) + 1
	return str(c)

    def _get_cpu_dies(self):
        try:
            return self._get_cpu_dies_dmi()
        except:
            return self._get_cpu_dies_cpuinfo()

    def _get_cpu_model(self):
        (out, err, ret) = justcall(['grep', 'model name', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def _get_serial(self):
        if self.container:
            return 'n/a'
        try:
            i = self.dmidecode.index('System Information')
        except ValueError:
            return 'Unknown'
        for l in self.dmidecode[i+1:]:
            if 'Serial Number:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def _get_enclosure(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Enclosure Name:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def _get_model(self):
        if self.container:
            return 'container'
        elif self.xenguest:
            return "Xen Virtual Machine (PVM)"
        for l in self.dmidecode:
            if 'Product Name:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def get_iscsi_hba_id(self):
        path = os.path.join(os.sep, 'etc', 'iscsi', 'initiatorname.iscsi')
        hba_id = None
        if os.path.exists(path):
            with open(path, 'r') as f:
                hba_id = f.read().split('=')[-1].strip()
        return hba_id
 
    def __get_hba(self):
        # fc / fcoe
        l = []
        import glob
        paths = glob.glob('/sys/class/fc_host/host*/port_name')
        for path in paths:
            host_link = '/'.join(path.split('/')[0:5])
            if '/eth' in os.path.realpath(host_link):
                hba_type = 'fcoe'
            else:
                hba_type = 'fc'
            with open(path, 'r') as f:
                hba_id = f.read().strip('0x').strip('\n')
            host = path.replace('/sys/class/fc_host/host', '')
            host = host[0:host.index('/')]

            l.append((hba_id, hba_type, host))

        # redhat 4 qla driver does not export hba portname in sysfs
        paths = glob.glob("/proc/scsi/qla2xxx/*")
        for path in paths:
            with open(path, 'r') as f:
                buff = f.read()
                for line in buff.split("\n"):
                    if "adapter-port" not in line:
                        continue
                    _l = line.split("=")
                    if len(_l) != 2:
                        continue
                    host = os.path.basename(path)
                    e = (_l[1].rstrip(";"), "fc", host)
                    if e not in l:
                        l.append(e)

        # iscsi
        path = os.path.join(os.sep, 'etc', 'iscsi', 'initiatorname.iscsi')
        hba_type = 'iscsi'
        hba_id = self.get_iscsi_hba_id()
        if hba_id is not None:
            l.append((hba_id, hba_type, ''))

        return l

    def _get_hba(self):
        return map(lambda x: (x[0], x[1]), self.__get_hba())

    def _get_targets(self):
        import glob
        # fc / fcoe
        l = []
        hbas = self.__get_hba()
        for hba_id, hba_type, host in hbas:
            if not hba_type.startswith('fc'):
                continue
            for target in glob.glob('/sys/class/fc_transport/target%s:*/port_name'%host):
                with open(target, 'r') as f:
                    tgt_id = f.read().strip('0x').strip('\n')
                l.append((hba_id, tgt_id))

        # iscsi
        hba_id = self.get_iscsi_hba_id()
        if hba_id is not None:
           cmd = ['iscsiadm', '-m', 'session']
           out, err, ret = justcall(cmd)
           if ret == 0:
               """
               tcp: [1] 192.168.231.141:3260,1 iqn.2000-08.com.datacore:sds1-1
               tcp: [2] 192.168.231.142:3260,1 iqn.2000-08.com.datacore:sds2-1
               """
               for line in out.split('\n'):
                   if len(line) == 0:
                       continue
                   l.append((hba_id, line.split()[-1]))

        return l

