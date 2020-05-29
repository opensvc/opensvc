import os
from subprocess import Popen, PIPE

from .asset import BaseAsset
from utilities.proc import justcall

os.environ['PATH'] += ":/opt/ignite/bin:/opt/propplus/bin"

class Asset(BaseAsset):
    def __init__(self, node):
        super(Asset, self).__init__(node)

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
        s = str(self.banks)
        if s == '0':
            s = self._get_mem_banks_ts99()
        return s

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
        n = self._get_cpu_cores_per_die()
        if n == 0:
            return str(self._get_cpu_dies_ts99())
        cores = int(self._get_cpu_cores())
        return str(cores // n)

    def _get_cpu_cores_per_die(self):
        n = 0
        i = 0
        for line in self.manifest:
            line = line.replace('(', '').replace(')', '')
            if 'Processors:' in line:
                i = 1
                continue
            if i > 0 and i < 4:
                i += 1
                if "core" not in line and "socket" not in line:
                    continue
                words = line.split()
                for j, w in enumerate(words):
                    if w == "socket":
                        try:
                            n = int(words[j-2])
                        except:
                            break
                    if 'core' in w:
                        try:
                            n = int(words[j-1])
                        except:
                            break
            elif i >= 4:
                break
        return n

    def _get_mem_banks_ts99(self):
        p = '/var/tombstones/ts99'
        if not os.path.exists(p):
            return '0'
        with open(p, 'r') as f:
            buff = f.read()
        lines = buff.split('\n')
        c = 0
        for line in lines:
            if "DIMM Error Information" in line:
                c += 1
        return str(c)

    def _get_cpu_dies_ts99(self):
        # count different serial numbers
        p = '/var/tombstones/ts99'
        if not os.path.exists(p):
            return 1
        with open(p, 'r') as f:
            buff = f.read()
        lines = buff.split('\n')
        serials = set()
        for line in lines:
            if "Cpu Serial Number" in line:
                serials.add(line.split()[-1])
        if len(serials) == 0:
            return 1
        return len(serials)

    def _get_cpu_model(self):
        s = self._get_cpu_model_manifest()
        if s == 'Unknown':
            s = self._get_cpu_model_ts99()
        return s

    def _get_cpu_model_ts99(self):
        p = '/var/tombstones/ts99'
        if not os.path.exists(p):
            return 'Unknown'
        with open(p, 'r') as f:
            buff = f.read()
        lines = buff.split('\n')
        for line in lines:
            if "CPU Module" in line:
                return line.strip().replace("CPU Module", "rev").replace('  ', ' ')
        return 'Unknown'

    def _get_cpu_model_manifest(self):
        marker = False
        for line in self.manifest:
            if 'Processors:' in line:
                marker = True
                continue
            if marker:
                if "processor" not in line:
                    return 'Unknown'
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
        try:
            return getattr(self, "hba")
        except AttributeError:
            pass
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
        return [{"hba_id": e[0], "hba_type": e[1]} for e in l]

    def _get_targets(self):
        hba = self.__get_hba()
        l = []
        for hba_id, hba_type, targets in hba:
            for target in targets:
                l.append({"hba_id": hba_id, "tgt_id": target})
        return l

