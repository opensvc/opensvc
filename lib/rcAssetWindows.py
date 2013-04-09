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
import sys
import platform
import datetime
from rcUtilities import justcall, which
from rcUtilitiesWindows import get_registry_value
import rcAsset
import ctypes
import wmi
from rcDiskInfoWindows import diskInfo

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
	s = set([])
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

