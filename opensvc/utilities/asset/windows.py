import sys
import platform
import datetime
import time

try:
    import ctypes
    import win32timezone
except ImportError:
    raise

from .asset import BaseAsset

from utilities.converters import convert_size
from utilities.lazy import lazy
from utilities.diskinfo import DiskInfo
from utilities.storage import Storage
from utilities.proc import justcall, which

class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [("dwLength", ctypes.c_uint),
                ("dwMemoryLoad", ctypes.c_uint),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("sullAvailExtendedVirtual", ctypes.c_ulonglong),]

    def __init__(self):
        # have to initialize this to the size of MEMORYSTATUSEX
        self.dwLength = 2*4 + 7*8     # size = 2 ints, 7 longs
        super(MEMORYSTATUSEX, self).__init__()

class Asset(BaseAsset):
    def __init__(self, node):
        self.node = node
        super(Asset, self).__init__(node)
        self.memstat = MEMORYSTATUSEX()
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(self.memstat))
        self.init()

    def init(self):
        self.wmi = self.node.wmi()

    def _get_tz(self):
        """
        return in fmt "+01:00"
        """
        mst = win32timezone.TimeZoneInfo.local()
        utcoff = datetime.datetime.now(mst).strftime("%z")
        return utcoff[:3] + ":" + utcoff[3:]

    def _get_mem_bytes(self):
        return str(self.memstat.ullTotalPhys // 1024 // 1024)

    def _get_mem_banks(self):
        md = len(self.wmi.WIN32_PhysicalMemory())
        return str(md)

    def _get_mem_slots(self):
        n = 0
        for a in self.wmi.WIN32_PhysicalMemoryArray():
            n += a.MemoryDevices
        return str(n)

    def _get_os_vendor(self):
        return 'Microsoft'

    def _get_os_name(self):
        return 'Windows'

    def _get_os_release(self):
        try:
            v = self.wmi.Win32_OperatingSystem()[0]
        except AttributeError:
            return "Unknown"
        s = v.Caption
        s = s.replace('Microsoft', '')
        s = s.replace('Windows', '')
        s = s.strip()
        return s

    def _get_os_kernel(self):
        try:
            v = sys.getwindowsversion()
        except AttributeError:
            return "Unknown"
        return ".".join(map(str, [v.major, v.minor, v.build]))

    def _get_os_arch(self):
        return platform.uname()[4]

    @lazy
    def cpuinfo(self):
        self.init()
        data = self.wmi.Win32_Processor()
        ret = Storage()
        ret.NumberOfCores = 0
        for p in data:
            try:
                ret.NumberOfCores += p.NumberOfCores,
            except Exception:
                ret.NumberOfCores += 1
            if ret.SocketDesignation is None:
                ret.SocketDesignation = p.SocketDesignation
            if ret.Name is None:
                ret.Name = p.Name
            if ret.MaxClockSpeed is None:
                ret.MaxClockSpeed = p.MaxClockSpeed
        return ret

    def _get_cpu_freq(self):
        return str(self.cpuinfo.MaxClockSpeed)

    def _get_cpu_cores(self):
        return str(self.cpuinfo.NumberOfCores)

    def _get_cpu_dies(self):
        return str(self.cpuinfo.SocketDesignation)

    def _get_cpu_model(self):
        return str(self.cpuinfo.Name)

    def _get_enclosure(self):
        for i in self.wmi.Win32_SystemEnclosure():
            name = i.Name
        return name

    def _get_serial(self):
        for i in self.wmi.Win32_ComputerSystemProduct():
            name = i.IdentifyingNumber
        return name

    def _get_model(self):
        for i in self.wmi.Win32_ComputerSystemProduct():
            name = i.Name
        return name

    def _get_hba(self):
        hbas = []
        self.di = DiskInfo()
        for index, portwwn, host in self.di._get_fc_hbas():
            hbas.append((portwwn, 'fc'))
        return hbas

    def _get_targets(self):
        maps = []
        if not which('fcinfo'):
            print('  fcinfo is not installed')
            return []
        for index, portwwn, host in self.di._get_fc_hbas():
            cmd = ['fcinfo', '/mapping', '/ai:'+index]
            out, err, ret = justcall(cmd)
            if ret != 0:
                print('error executing', ' '.join(cmd), out, err, ret)
                continue
            for line in out.split('\n'):
                if not line.startswith('(x'):
                    continue
                l = line.split()
                if len(l) < 3:
                    continue
                tgtportwwn = l[2].strip(',').replace(':', '')
                if (portwwn, tgtportwwn) in maps:
                    continue
                maps.append((portwwn, tgtportwwn))
        return maps

    def get_boot_id(self):
        payload = self.wmi.Win32_PerfFormattedData_PerfOS_System()
        uptime = payload[-1].SystemUpTime
        return str((int(time.time()) - int(uptime)) // 2)

    def get_last_boot(self):
        self.init()
        payload = self.wmi.Win32_PerfFormattedData_PerfOS_System()
        uptime = payload[-1].SystemUpTime
        try:
            last = datetime.datetime.now() - datetime.timedelta(seconds=int(uptime))
        except:
            return
        last = last.strftime("%Y-%m-%d")
        return {
            "title": "last_boot",
            "value": last,
            "source": "probe",
        }

    def get_hardware(self):
        devs = []
        devs += self.get_hardware_devs()
        devs += self.get_hardware_mem()
        return devs

    def get_hardware_mem(self):
        """
        Get-WmiObject -Class "win32_PhysicalMemory"
        instance of Win32_PhysicalMemory
        {
        Attributes = 0;
        BankLabel = "";
        Capacity = "2147483648";
        Caption = "Memoire physique";
        ConfiguredClockSpeed = 0;
        ConfiguredVoltage = 0;
        CreationClassName = "Win32_PhysicalMemory";
        Description = "Memoire physique";
        DeviceLocator = "DIMM 0";
        FormFactor = 8;
        Manufacturer = "QEMU";
        MaxVoltage = 0;
        MemoryType = 9;
        MinVoltage = 0;
        Name = "Memoire physique";
        SMBIOSMemoryType = 7;
        Tag = "Physical Memory 0";
        TypeDetail = 2;
        };
        """
        devs = []
        dev = None
        path = []
        cla = []
        desc = []
        payload = self.wmi.WIN32_PhysicalMemory()
        for a in payload:
            path = []
            cla = []
            desc = []
            dev = {
                    "type": "mem",
                    "path": "",
                    "class": "",
                    "description": "",
                    "driver": "",
                }
            path.append(a.DeviceLocator)
            if len(a.BankLabel) > 0:
                path.append(a.BankLabel)
            if a.Description is not None:
                desc.append(a.Description)
            if a.Manufacturer is not None:
                desc.append(a.Manufacturer)
            size = str(convert_size(a.Capacity, _to="GB"))+'GB'
            cla.append(size)
            if dev is not None:
                dev["path"] = " ".join(path)
                dev["class"] = " ".join(cla)
                dev["description"] = " ".join(desc)
                devs.append(dev)
        return devs

    def get_hardware_devs(self):
        """
        Get-WmiObject -Class "Win32_PnpSignedDriver"
        """
        devs = []
        dev = None
        path = []
        cla = []
        desc = []
        payload = self.wmi.Win32_PnpSignedDriver()
        unknowncpt = 0
        for a in payload:
            path = []
            cla = []
            desc = []
            type = []
            driver = []
            dev = {
                    "type": "",
                    "path": "",
                    "class": "",
                    "description": "",
                    "driver": "",
                }
            if a.Description is not None:
                desc.append(a.Description)
            if a.Caption is not None:
                desc.append(a.Caption)
            if a.FriendlyName is not None:
                desc.append(a.FriendlyName)
            if a.Manufacturer is not None:
                desc.append(a.Manufacturer)
            if len(desc) == 0 and a.DeviceID is not None:
                desc.append(a.DeviceID)
                if 'XPS Document Writer' in a.DeviceID:
                    type.append('xps printer')
                if 'Print to PDF' in a.DeviceID:
                    type.append('pdf printer')
            if a.DeviceClass is not None:
                cla.append(a.DeviceClass)
            if a.Location is not None:
                if 'PCI bus' in str(a.Location):
                    type.append('pci')
                    pciinfo = a.Location.split(',')
                    pcibus = pciinfo[0].split(' ')[-1]
                    device = pciinfo[1].split(' ')[-1]
                    function = pciinfo[2].split(' ')[-1]
                    string = str("%02d"%int(pcibus) + ':' + "%02d"%int(device) + '.' + function)
                    path.append(string)
                else:
                    path.append(a.Location)
            if a.PDO is not None and len(path) == 0:
                path.append(a.PDO)
            if a.DriverProviderName is not None:
                driver.append(a.DriverProviderName)
            if a.InfName is not None:
                driver.append(a.InfName)
            if a.DriverVersion is not None:
                driver.append(a.DriverVersion)
            if len(type) == 0 and a.DeviceClass is not None:
                type.append(a.DeviceClass.lower())
            if len(type) == 0:
                type.append("unknown [" + str(unknowncpt) + "]")
                unknowncpt+=1
            if len(path) == 0 and a.DeviceID is not None:
                path.append(a.DeviceID)
            if dev is not None:
                dev["path"] = " ".join(path)
                dev["type"] = " ".join(type)
                dev["class"] = " ".join(cla)
                dev["driver"] = " ".join(driver)
                dev["description"] = " ".join(desc)
                devs.append(dev)
        return devs
