#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>'
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

import rcDiskInfo
import wmi
from rcUtilities import justcall, which

class diskInfo(rcDiskInfo.diskInfo):

    def __init__(self):
        self.h = {}
        self.fcluns = {}
        self.wmi = wmi.WMI()

    def scan_mapping(self):
        if len(self.fcluns) > 0:
            return

        if not which('fcinfo'):
            return

        for index, portwwn, host in self._get_fc_hbas():
            cmd = ['fcinfo', '/mapping', '/ai:'+index]
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            lines = out.split('\n')
            for i, line in enumerate(lines):
                if line.startswith('(  '):
                    l = line.split()
                    if len(l) < 3:
                        continue
                    bus = int(l[-3].strip(','))
                    target = int(l[-2].strip(','))
                    lun = int(l[-1].strip(')'))
                    _index = (host, bus, target, lun)
                elif line.startswith('(cs:'):
                    l = line.split()
                    if len(l) < 2:
                        continue
                    wwid = l[-1].strip(')')
                    self.fcluns[_index] = dict(wwid=wwid)

    def scan(self):
        self.scan_mapping()

        vid = 'unknown'
        pid = 'unknown'
        wwid = 'unknown'
        size = 'unknown'

        for drive in self.wmi.WIN32_DiskDrive():
            id = drive.DeviceID
            vid = str(drive.Manufacturer)
            pid = str(drive.Caption)
            serial = str(drive.SerialNumber)
            size = int(drive.Size) // 1024 // 1024
            host = drive.SCSIPort
            bus = drive.SCSIBus
            target = drive.SCSITargetId
            lun = drive.SCSILogicalUnit

            d = dict(id=id,
                     vid=vid,
                     pid=pid,
                     wwid=wwid,
                     serial=serial,
                     host=host,
                     bus=bus,
                     target=target,
                     lun=lun,
                     size=size)

            d['wwid'] = self.get_wwid(d)
            if d['wwid'] is None:
                d['wwid'] = d['serial']

            self.h[id] = d

    def get_wwid(self, d):
        index = (d['host'], d['bus'], d['target'], d['lun'])
        if index not in self.fcluns:
            return None
        return self.fcluns[index]['wwid']

    def get(self, id, prop):
        if len(self.h) == 0:
            self.scan()
        if id not in self.h:
            return None
        return self.h[id][prop]

    def disk_id(self, dev):
        return self.get(dev, 'wwid')

    def disk_vendor(self, dev):
        return self.get(dev, 'vid')

    def disk_model(self, dev):
        return self.get(dev, 'pid')

    def disk_size(self, dev):
        return self.get(dev, 'size')

    def _get_fc_hbas(self):
        hbas = []
        if not which('fcinfo'):
            return []
        cmd = ['fcinfo']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []
        for line in out.split('\n'):
            if 'PortWWN' not in line:
                continue
            l = line.split()
            i = l.index('PortWWN:')
            if len(l) < i+2:
                continue
            index = l[0].split('-')[-1].strip(':')
            portwwn = l[i+1].replace(':', '')
            host = int(l[-1].split('Scsi')[-1].strip(':'))
            hbas.append((index, portwwn, host))
        return hbas

