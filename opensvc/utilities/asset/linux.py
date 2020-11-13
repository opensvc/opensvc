import os
import re

from .asset import BaseAsset
from utilities.lazy import lazy
from utilities.storage import Storage
from utilities.proc import justcall, which
from utilities.string import bdecode

def is_container():
    p = '/proc/1/environ'
    if not os.path.exists(p):
        return False
    with open(p, 'r') as f:
        buff = f.read()
    if "container=lxc" in bdecode(buff):
        return True
    return False

class Asset(BaseAsset):
    def __init__(self, node):
        super(Asset, self).__init__(node)
        self.container = is_container()
        self.detect_xen()
        if self.container:
            self.dmidecode = []
        else:
            out, err, ret = justcall(['dmidecode'])
            if ret != 0:
                self.dmidecode = []
            else:
                self.dmidecode = out.split('\n')

    @lazy
    def os_release(self):
        os_release_f = os.path.join(os.sep, "etc", "os-release")
        data = Storage()
        if not os.path.exists(os_release_f):
            return data
        with open(os_release_f, "r") as filep:
            for line in filep.readlines():
                line = line.strip("\n")
                try:
                    var, val = line.split("=", 1)
                except:
                    continue
                data[var.lower()] = val.strip('"')
        return data

    def _get_mem_bytes_esx(self):
        cmd = ['vmware-cmd', '-s', 'getresource', 'system.mem.totalMem']
        out, err, ret = justcall(cmd)
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
        from utilities.converters import convert_size
        cmd = ['virsh', 'nodeinfo']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        for line in lines:
            if 'Memory size' not in line:
                continue
            l = line.split(":", 1)
            if len(l) < 2:
                continue
            return str(convert_size(l[-1], _to="MB"))
        return '0'

    def _get_mem_bytes_xm(self):
        cmd = ['xm', 'info']
        out, err, ret = justcall(cmd)
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
        out, err, ret = justcall(cmd)
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
        c = os.path.join(os.sep, 'sys', 'hypervisor', 'uuid')
        self.xenguest = False
        self.xenhv = False
        if not os.path.exists(c):
            return
        with open(c, 'r') as f:
            if '00000000-0000-0000-0000-000000000000' in f.read():
                self.xenhv = True
            else:
                self.xenguest = True

    def is_esx_hv(self):
        return which('vmware-cmd')

    def _get_mem_bytes(self):
        if self.xenhv:
            return self._get_mem_bytes_hv()
        elif self.is_esx_hv():
            s = self._get_mem_bytes_esx()
            if s == '0':
                return self._get_mem_bytes_phy()
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
        vendors = {
            "alpine": "Alpine",
            "debian": "Debian",
            "ubuntu": "Ubuntu",
            "arch": "Arch",
            "vmware": "VMware",
            "oracle": "Oracle",
            "sles": "SuSE",
            "opensuse": "SuSE",
            "rhel": "Red Hat",
            "centos": "CentOS",
            "fedora": "Fedora",
            "caasp": "SuSE",
            "gentoo": "Gentoo",
        }
        if self.os_release.id in vendors:
            return vendors.get(self.os_release.id, "")
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
        if os.path.exists('/etc/alpine-release'):
            return "Alpine"
        if self.os_release.name:
            return self.os_release.name
        return 'Unknown'

    def _get_os_release_lsb(self):
        if not os.path.exists('/etc/lsb-release'):
            return
        r = None
        with open('/etc/lsb-release') as f:
            for line in f.readlines():
                if 'DISTRIB_RELEASE' in line:
                    r = line.split('=')[-1].replace('\n','').strip('"')
                    if r:
                        break
                if 'DISTRIB_DESCRIPTION' in line:
                    r = line.split('=')[-1].replace('\n','').strip('"')
                    if r:
                        break
        if r:
            r = r.replace(self._get_os_vendor(), '').strip()
        return r

    def _get_os_release_os_release(self):
        r = self.os_release.pretty_name
        if not r:
            return
        v = self._get_os_vendor()
        if v:
            pattern = re.compile(v, flags=re.I)
            r = pattern.sub("", r)
        return r.strip()

    def _get_os_release_debian_version(self):
        if not os.path.exists('/etc/debian_version'):
            return
        with open('/etc/debian_version') as f:
            r = f.read().strip()
        if r == "":
            return
        return r

    def _get_os_release(self):
        r = self._get_os_release_os_release()
        if r and r not in (
           "/Linux",
           "Linux 7 (Core)" # centos7 poor pretty_name
        ):
            return r
        files = ['/etc/debian_version',
                 '/etc/vmware-release',
                 '/etc/oracle-release',
                 '/etc/redhat-release',
                 '/etc/gentoo-release']
        if os.path.exists('/etc/SuSE-release'):
            v = []
            with open('/etc/SuSE-release') as f:
                for line in f.readlines():
                    if 'VERSION' in line:
                        v += [line.split('=')[-1].replace('\n','').strip('" ')]
                    if 'PATCHLEVEL' in line:
                        v += [line.split('=')[-1].replace('\n','').strip('" ')]
            return '.'.join(v)
        if os.path.exists('/etc/alpine-release'):
            with open('/etc/alpine-release') as f:
                return f.read().strip()
        r = self._get_os_release_lsb()
        if r:
            return r
        r = self._get_os_release_debian_version()
        if r:
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
                return out.split('\n')[0].replace(self._get_os_vendor(), '').replace("GNU/Linux", "").replace("Linux", "").replace("release", "").strip()
        return 'Unknown'

    def _get_os_kernel(self):
        return os.uname()[2]

    def _get_os_arch(self):
        return os.uname()[4]

    def _get_cpu_freq(self):
        p = '/proc/cpuinfo'
        if not os.path.exists(p):
            return 'Unknown'
        with open(p, 'r') as f:
            for line in f.readlines():
                if 'cpu MHz' in line:
                    return line.split(':')[1].strip().split('.')[0]

        p_raspbian = '/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq'
        if os.path.exists(p_raspbian):
            (out, err, ret) = justcall(['cat', p_raspbian])
            if ret != 0:
                return 'Unknown'
            return out[:-4]

        return 'Unknown'

    def _get_cpu_cores(self):
        try:
            with open('/proc/cpuinfo') as f:
                lines = f.readlines()
        except:
            return '0'
        phy = {}
        for line in lines:
            if 'physical id' in line:
                id = line.split(":")[-1].strip()
                if id not in phy:
                    phy[id] = []
            elif 'core id' in line:
                coreid = line.split(":")[-1].strip()
                if coreid not in phy[id]:
                    phy[id].append(coreid)
        n_cores = 0
        for id, coreids in phy.items():
            n_cores += len(coreids)
        if n_cores == 0:
            return self._get_cpu_dies()
        return str(n_cores)

    def _get_cpu_dies_dmi(self):
        if self.container:
            return 'n/a'
        n = 0
        for l in self.dmidecode:
            if 'Processor Information' in l:
                n += 1
        return str(n)

    def _get_cpu_dies_cpuinfo(self):
        try:
            with open('/proc/cpuinfo') as f:
                lines = f.readlines()
        except:
            return '0'
        _lines = set([l for l in lines if 'physical id' in l])
        n_dies = len(_lines)
        if n_dies > 0:
            return str(n_dies)
        # vmware do not show processor physical id
        _lines = [l for l in lines if 'processor' in l]
        n_dies = len(_lines)
        return str(n_dies)

    def _get_cpu_threads(self):
        try:
            with open('/proc/cpuinfo') as f:
                lines = f.readlines()
        except:
            return '0'
        lines = [l for l in lines if 'physical id' in l]
        n_threads = len(lines)
        if n_threads == 0:
            return self._get_cpu_dies()
        return str(n_threads)

    def _get_cpu_dies(self):
        n = self._get_cpu_dies_cpuinfo()
        if n == '0':
            n = self._get_cpu_dies_dmi()
        return n

    def _get_cpu_model(self):
        (out, err, ret) = justcall(['grep', 'model name', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def _get_serial_1(self):
        try:
            i = self.dmidecode.index('System Information')
        except ValueError:
            return 'Unknown'
        for l in self.dmidecode[i+1:]:
            if 'Serial Number:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def _get_serial_2(self):
        """ Dell poweredge 2500 are known to be in this case
        """
        try:
            i = self.dmidecode.index('Chassis Information')
        except ValueError:
            return 'Unknown'
        for l in self.dmidecode[i+1:]:
            if 'Serial Number:' in l:
                return l.split(':')[-1].strip()
        return 'Unknown'

    def _get_serial_raspbian(self):
        """ Raspbian serial is in /proc/cpuinfo
        """
        (out, err, ret) = justcall(['grep', '^Serial', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def _get_serial(self):
        if self.container:
            return 'n/a'
        serial = self._get_serial_1()
        if serial in ('Unknown', 'Not Specified'):
            serial = self._get_serial_2()
        if serial in ('Unknown') and self.os_release['id'] == 'raspbian':
            serial = self._get_serial_raspbian()
        return serial

    def _get_bios_version(self):
        if self.container:
            return 'n/a'
        v = ""
        rev = ""
        try:
            i = self.dmidecode.index('BIOS Information')
        except ValueError:
            return ''
        for l in self.dmidecode[i+1:]:
            if 'Version:' in l:
                v = l.split(':')[-1].strip()
                break
        for l in self.dmidecode[i+1:]:
            if 'BIOS Revision:' in l:
                rev = l.split(':')[-1].strip()
                break
        if len(rev) > 1 and not v.startswith(rev):
            return v+" "+rev
        return v

    def _get_sp_version(self):
        if self.container:
            return 'n/a'
        sp_version = self._get_sp_version_ipmi()
        if sp_version:
            return sp_version
        return ''

    def _get_sp_version_ipmi(self):
        if which("ipmitool") is None:
            return
        cmd = ["ipmitool", "mc", "info"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        for l in out.splitlines():
            if 'Firmware Revision' in l:
                v = l.split(' : ')[-1].strip()
                return v
        return v

    def _get_enclosure(self):
        if self.container:
            return 'n/a'
        for l in self.dmidecode:
            if 'Enclosure Name:' in l:
                return l[l.index(":")+1:].strip()
        return 'Unknown'

    def _get_manufacturer(self):
        if self.container:
            return ""
        elif self.xenguest and len(self.dmidecode) < 5:
            return ""
        out, err, ret = justcall(["dmidecode", "-s", "system-manufacturer"])
        if ret != 0:
            return ""
        return out.strip()

    def _get_revision_raspbian(self):
        (out, err, ret) = justcall(['grep', '^Revision', '/proc/cpuinfo'])
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        l = lines[0].split(':')
        return l[1].strip()

    def _get_model(self):
        if self.container:
            return 'container'
        elif self.xenguest and len(self.dmidecode) < 5:
            return "Xen Virtual Machine (PVM)"
        elif self.os_release['id'] == 'raspbian':
            model = self._get_revision_raspbian()
            return model
        for l in self.dmidecode:
            if 'Product Name:' in l:
                return l[l.index(":")+1:].strip()
        return 'Unknown'

    def get_iscsi_hba_id(self):
        path = os.path.join(os.sep, 'etc', 'iscsi', 'initiatorname.iscsi')
        hba_id = None
        if os.path.exists(path):
            with open(path, 'r') as f:
                hba_id = f.read().split('=')[-1].strip()
        return hba_id

    def _get_hba(self):
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

        # gce
        if self._get_model() == "Google":
            from env import Env
            l.append((Env.nodename, "virtual", ''))

        return [{"hba_id": e[0], "hba_type": e[1], "host": e[2]} for e in l]

    def _get_targets(self):
        def port_not_present(target):
            fpath = os.path.dirname(target)
            fpath = os.path.join(fpath, "port_state")
            try:
                with open(fpath,"r") as f:
                    buff = f.read().strip()
            except Exception:
                return False
            return buff == "Not Present"

        import glob
        # fc / fcoe
        l = []
        hbas = self._get_hba()
        for hba in hbas:
            if not hba["hba_type"].startswith('fc'):
                continue
            targets = glob.glob('/sys/class/fc_transport/target%s:*/port_name'%hba["host"])
            targets += glob.glob('/sys/class/fc_remote_ports/rport-%s:*/port_name'%hba["host"])
            for target in targets:
                with open(target, 'r') as f:
                    tgt_id = f.read().strip('0x').strip('\n')
                if port_not_present(target):
                    continue
                if (hba["hba_id"], tgt_id) not in l:
                    l.append((hba["hba_id"], tgt_id))

        # iscsi
        hba_id = self.get_iscsi_hba_id()
        if hba_id is not None:
            cmd = ['iscsiadm', '-m', 'session']
            out, err, ret = justcall(cmd)
            if ret == 0:
                """
                tcp: [1] 192.168.231.141:3260,1 iqn.2000-08.com.datacore:sds1-1
                tcp: [2] 192.168.231.142:3260,1 iqn.2000-08.com.datacore:sds2-1 (non-flash)
                """
                for line in out.splitlines():
                    if len(line) == 0:
                        continue
                    line = line.replace(" (non-flash)", "")
                    l.append((hba_id, line.split()[-1]))

        # gce
        if self._get_model() == "Google":
            try:
                cmd = ["gcloud", "compute", "regions", "list", "-q", "--format", "json"]
                out, err, ret = justcall(cmd)
                import json
                from env import Env
                data = json.loads(out)
                hba_id = Env.nodename
                for region in data:
                    i = region["selfLink"].index("/projects")
                    tgt_id = region["selfLink"][i:].replace("/projects", "").replace("/regions", "")
                    l.append((hba_id, tgt_id))
            except:
                pass

        return [{"hba_id": e[0], "tgt_id": e[1]} for e in l]

    def get_hardware(self):
        devs = []
        devs += self.get_hardware_mem()
        devs += self.get_hardware_pci()
        return devs

    def get_hardware_mem(self):
        out, err, ret = justcall(["dmidecode", "-t", "memory"])
        if ret != 0 or "SMBIOS nor DMI" in out:
            return []
        devs = []
        dev = {}
        path = []
        cla = []
        desc = []
        for line in out.splitlines():
            if line.strip() == "Memory Device":
                # new mem device
                if dev:
                    dev["path"] = " ".join(path)
                    dev["class"] = " ".join(cla)
                    dev["description"] = " ".join(desc)
                    devs.append(dev)
                dev = {
                    "type": "mem",
                    "path": "",
                    "class": "",
                    "description": "",
                    "driver": "",
                }
                path = []
                desc = []
                cla = []
            elif "Locator:" in line:
                path.append(line[line.index(":")+1:].strip())
            elif "Bank Locator:" in line:
                path.append(line[line.index(":")+1:].strip())
            elif "Type:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "Unknown":
                    cla.append(s)
            elif "Type Detail:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "None":
                    cla.append(s)
            elif "  Speed:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "Unknown":
                    cla.append(s)
            elif "Size:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "Unknown":
                    cla.append(s)
            elif "Manufacturer:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "Unknown":
                    desc.append(s)
            elif "Part Number:" in line:
                s = line[line.index(":")+1:].strip()
                if s != "Unknown":
                    desc.append(s)
        if dev:
            dev["path"] = " ".join(path)
            dev["class"] = " ".join(cla)
            dev["description"] = " ".join(desc)
            devs.append(dev)
        return devs
 
    def get_hardware_pci(self):
        out, err, ret = justcall(["lspci", "-v"])
        if ret != 0:
            return []
        devs = []
        dev = {}
        for line in out.splitlines():
            if re.match(r"^\w", line):
                # new pci device
                if dev:
                    devs.append(dev)
                words = line.split()
                path = words.pop(0)
                line = " ".join(words)
                cla = line[:line.index(":")]
                description = line[line.index(":")+1:].strip()
                dev = {
                    "type": "pci",
                    "path": path,
                    "class": cla,
                    "description": description,
                    "driver": "",
                }
            elif "Kernel driver in use:" in line:
                dev["driver"] = line[line.index(":")+1:].strip()
        if dev:
            devs.append(dev)
        return devs
                
    def get_boot_id(self):
        fpath = "/proc/sys/kernel/random/boot_id"
        if os.path.exists(fpath):
            with open(fpath, "r") as f:
                return f.read().strip()
        fpath = "/proc/stat"
        if os.path.exists(fpath):
            with open(fpath, "r") as f:
                for line in f.readlines():
                    if line.startswith("btime "):
                        return line.split()[-1]
        return super(Asset, self).get_boot_id()

if __name__ == "__main__":
    from env import Env
    import json
    print(json.dumps(Asset(Env.nodename).get_asset_dict(), indent=4))
