import os
import sys
import platform
import datetime
from rcUtilities import justcall, which, try_decode
from rcUtilitiesWindows import get_registry_value
import rcAsset
import ctypes
import wmi
from rcDiskInfoWindows import diskInfo
from converters import convert_size

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
        return super(MEMORYSTATUSEX, self).__init__()


class Asset(rcAsset.Asset):
    def __init__(self, node):
        self.w = wmi.WMI()
        self.cpuinfo = self.w.Win32_Processor()
        rcAsset.Asset.__init__(self, node)
        self.memstat = MEMORYSTATUSEX()
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(self.memstat))

    def _get_tz(self):
        # TODO: return in fmt "+01:00"
        return

    def _get_mem_bytes(self):
        return str(self.memstat.ullTotalPhys // 1024 // 1024)

    def _get_mem_banks(self):
        md = len(self.w.WIN32_PhysicalMemory())
        return str(md)

    def _get_mem_slots(self):
        n = 0
        for a in self.w.WIN32_PhysicalMemoryArray():
            n += a.MemoryDevices
        return str(n)

    def _get_os_vendor(self):
        return 'Microsoft'

    def _get_os_name(self):
        return 'Windows'

    def _get_os_release(self):
        v = sys.getwindowsversion()
        product = {
         1: 'Workstation',
         2: 'Domain Controller',
         3: 'Server',
        }
        s = platform.release()
        s = s.replace('Server', ' Server')
        s = s.replace('ServerR', ' Server R')
        s = s.replace('Workstation', ' Workstation')
        s += " %s" % v.service_pack
        return s

    def _get_os_kernel(self):
        v = sys.getwindowsversion()
        return ".".join(map(str, [v.major, v.minor, v.build]))

    def _get_os_arch(self):
        return platform.uname()[4]

    def _get_cpu_freq(self):
        for i in self.cpuinfo:
            cpuspeed = i.MaxClockSpeed
        return str(cpuspeed)

    def _get_cpu_cores(self):
        n = 0
        for p in self.cpuinfo:
            try:
                cores = p.NumberOfCores
            except:
                cores = 1
            n += cores
        return str(n)

    def _get_cpu_dies(self):
        s = set()
        for p in self.cpuinfo:
            s.add(p.SocketDesignation)
        n = len(s)
        return str(n)

    def _get_cpu_model(self):
        for i in self.cpuinfo:
            cputype = i.Name
        return cputype

    def _get_enclosure(self):
        for i in self.w.Win32_SystemEnclosure():
            name = i.Name
        return name

    def _get_serial(self):
        for i in self.w.Win32_ComputerSystemProduct():
            name = i.IdentifyingNumber
        return name

    def _get_model(self):
        for i in self.w.Win32_ComputerSystemProduct():
            name = i.Name
        return name

    def _get_hba(self):
        hbas = []
        self.di = diskInfo()
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

    def get_last_boot(self):
        payload = self.w.Win32_PerfFormattedData_PerfOS_System()
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
        '''
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
		'''
        devs = []
        dev = None
        path = []
        cla = []
        desc = []
        payload = self.w.WIN32_PhysicalMemory()
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
            desc.append(a.Description)
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
        '''
        Get-WmiObject -Class "Win32_PnpSignedDriver"
		'''
        devs = []
        dev = None
        path = []
        cla = []
        desc = []
        payload = self.w.Win32_PnpSignedDriver()
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
            if a.Manufacturer is not None:
                desc.append(a.Manufacturer)
            if len(desc) == 0:
                desc.append(a.DeviceID)
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
            if a.DriverProviderName is not None:
                driver.append(a.DriverProviderName)
            if a.InfName is not None:
                driver.append(a.InfName)
            if a.DriverVersion is not None:
                driver.append(a.DriverVersion)    
            if dev is not None:
                dev["path"] = " ".join(path)
                dev["type"] = " ".join(type)
                dev["class"] = " ".join(cla)
                dev["driver"] = " ".join(driver)
                dev["description"] = " ".join(desc)
                devs.append(dev)
        return devs