import os

from .linux import Asset as BaseAsset
from utilities.proc import justcall

class Asset(BaseAsset):
    def _get_mem_bytes(self):
        cmd = ['sysctl', 'hw.realmem']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return '0'
        lines = out.split('\n')
        if len(lines) < 1:
            return '0'
        line = lines[0].split()
        if len(line) < 2:
            return '0'
        mb = int(line[-1])
        return str(mb/1024/1024)

    def _get_os_vendor(self):
        return 'FreeBSD'

    def _get_os_release(self):
        return self._get_os_kernel()

    def _get_os_arch(self):
        cmd = ['sysctl', 'hw.machine_arch']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return line[-1]

    def _get_cpu_model(self):
        cmd = ['sysctl', 'hw.model']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return " ".join(line[1:])

    def _get_cpu_cores(self):
        cmd = ['sysctl', 'hw.ncpu']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return line[-1]

    def _get_cpu_freq(self):
        cmd = ['sysctl', 'hw.clockrate']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        lines = out.split('\n')
        if len(lines) < 1:
            return 'Unknown'
        line = lines[0].split()
        if len(line) < 2:
            return 'Unknown'
        return line[-1]

    def get_boot_id(self):
        try:
            return super(Asset, self).get_boot_id()
        except:
            # /proc might not be mounted
            return str(os.path.getmtime("/dev"))

