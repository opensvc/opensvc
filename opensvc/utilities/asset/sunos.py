import json
import os
import re

from .asset import BaseAsset
from foreign.six.moves.urllib.parse import quote # pylint: disable=import-error
from utilities.subsystems.zone import is_zone
from utilities.proc import justcall, which

class Asset(BaseAsset):
    def __init__(self, node=None):
        super(Asset, self).__init__(node)
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
        try:
            with open("/var/pkg/state/installed/catalog.summary.C", "r") as filep:
                data = json.load(filep)
            version = data["solaris"]["entire"][0]["version"]
            fpath = "/var/pkg/cache/publisher/solaris/pkg/entire/%s/manifest.set" % quote(version)
            with open(fpath, "r") as filep:
                buff = filep.read()
            for l in buff.splitlines():
                if 'set name=pkg.human-version' in l:
                    hv = l.split('"')[-2]
                    if 'SRU' in hv:
                        return ' '.join([lines[0], 'SRU', hv.split()[-1]])
                    elif lines[0] in hv:
                        return hv.split()[-1]
                    else:
                        return ' '.join([lines[0], hv.split()[-1]])
        except Exception:
            pass
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
        prev = ""
        for w in out.split():
            if 'MHz)' in w:
                return prev
            prev = w
        out, err, ret = justcall(['kstat', 'cpu_info'])
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
        core_ids = set()
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
        return [{"hba_id": e[0], "hba_type": e[1]} for e in l]

    def _get_targets(self):
        l = self.__get_hba()
        m = []
        for hba_id, hba_type, targets in l:
             for target in targets:
                 m.append({"hba_id": hba_id, "tgt_id": target})
        return m

    def _get_bios_version(self):
        arch = self._get_os_arch().lower()
        if arch.startswith("sun4"):
            return self._get_bios_version_sparc()
        else:
            return self._get_bios_version_intel()

    def _get_bios_version_sparc(self):
        for l in self.prtdiag:
            if l.startswith("OBP "):
                v = l.replace("OBP ", "").strip()
                v = re.sub(' [0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}', '', v)
                return v
        return ''

    def _get_bios_version_intel(self):
        if which('smbios') is None:
            return ''
        out, err, ret = justcall(['smbios'])
        if ret != 0:
            return ''
        try:
            i = out.index('BIOS information')
        except ValueError:
            return ''
        for l in out[i:].splitlines():
            if 'Version String' in l:
                return l.split(':')[-1].strip()
        return ''

    def get_boot_id_zone(self):
        pid = self.zsched_pid()
        if pid is None:
            return
        return str(os.path.getmtime("/proc/%s" % pid))

    def zsched_pid(self):
        cmd = ["pgrep", "zsched"]
        out, err, ret = justcall(cmd)
        pid = out.split()[0]
        if pid == "":
            return
        return pid

    def get_boot_id(self):
        if self.zone:
            return self.get_boot_id_zone()
        else:
            return super(Asset, self).get_boot_id()

if __name__ == "__main__":
    print(Asset()._get_cpu_model())
