from .asset import BaseAsset
import core.exceptions as ex
from utilities.proc import justcall

class Asset(BaseAsset):
    def __init__(self, node):
        super(Asset, self).__init__(node)
        self.info = {}
        (out, err, ret) = justcall(['system_profiler', 'SPHardwareDataType', 'SPSoftwareDataType'])
        if ret == 0:
            for line in out.split('\n'):
                l = line.split(':')
                if len(l) != 2: continue
                self.info[l[0].strip()] = l[1].strip()
        self.memslots = 0
        self.membanks = 0
        self._collect_memory_info()

    def _collect_memory_info(self):
        (out, err, ret) = justcall(['system_profiler', 'SPMemoryDataType'])
        if ret == 0:
            inBlock = False
            for line in out.split('\n'):
                line = line.strip()
                if not inBlock and line.startswith("BANK"):
                    inBlock = True
                    self.memslots += 1
                if inBlock and line.startswith("Status"):
                    l = line.split(':')
                    if 'OK' in l[1].strip():
                        self.membanks += 1
                    inBlock = False

    def _get_mem_bytes(self):
        if 'Memory' not in self.info:
            return '0'
        m = self.info['Memory'].split()
        size = int(m[0])
        unit = m[1]
        if unit == 'GB':
            size = size * 1024
        elif unit == 'MB':
            pass
        else:
            raise ex.Error("unexpected memory format")
        return str(size)

    def _get_mem_banks(self):
        return str(self.membanks)

    def _get_mem_slots(self):
        return str(self.memslots)

    def _get_os_vendor(self):
        return 'Apple'

    def _get_os_release(self):
        if 'System Version' in self.info:
            return self.info['System Version']
        (out, err, ret) = justcall(['uname', '-r'])
        if ret != 0:
            return 'Unknown'
        return out.split()[0]

    def _get_os_kernel(self):
        if 'Kernel Version' not in self.info:
            return '0'
        return self.info['Kernel Version']

    def _get_os_arch(self):
        cmd = ['uname', '-m']
        (out, err, ret) = justcall(cmd)
        if ret != 0:
            return 'Unknown'
        return out.split('\n')[0]

    def _get_cpu_freq(self):
        if 'Processor Speed' not in self.info:
            return '0'
        return self.info['Processor Speed']

    def _get_cpu_cores(self):
        if 'Total Number of Cores' not in self.info:
            return '0'
        return self.info['Total Number of Cores']

    def _get_cpu_dies(self):
        if 'Number of Processors' not in self.info:
            return '0'
        return self.info['Number of Processors']

    def _get_cpu_model(self):
        if 'Processor Name' not in self.info:
            return '0'
        return self.info['Processor Name']

    def _get_serial(self):
        if 'Hardware UUID' not in self.info:
            return '0'
        return self.info['Hardware UUID']

    def _get_model(self):
        if 'Model Name' not in self.info:
            return '0'
        return self.info['Model Name']

