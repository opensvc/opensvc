import os
from subprocess import *

import core.exceptions as ex
from utilities.proc import which
from . import BaseDiskScsireserv

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "scsireserv"


class DiskScsireserv(BaseDiskScsireserv):
    def __init__(self, **kwargs):
        super(DiskScsireserv, self).__init__(**kwargs)
        self.prtype = "wero"
        self.disk_id = {}
        self.itn = {}

    def get_devs(self):
        if len(self.devs) > 0:
            return
        for dev in self.peer_resource.sub_devs():
            dev = dev.replace('/disk/', '/rdisk/')
            self.devs[dev] = [dev]

    def scsireserv_supported(self):
        if which('scu') is None:
            return False
        return True

    def ack_unit_attention(self, d):
        return 0

    def get_disk_ids(self):
        if len(self.disk_id) > 0:
            return
        cmd = ['hwmgr', 'show', 'scsi']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error(err)
        for line in out.split('\n'):
            v = line.split()
            if len(v) < 7:
                continue
            if v[3] != "disk":
                continue
            if not v[7].startswith("dsk"):
                continue
            self.disk_id[v[7] + 'c'] = v[0].strip(":")

    def get_itns(self):
        if len(self.itn) > 0:
            return
        self.get_disk_ids()
        for disk in self.disk_id:
            self.get_itn(disk)

    def get_itn(self, disk):
        if disk in self.itn:
            return
        self.itn[disk] = []
        if disk not in self.disk_id:
            return
        id = self.disk_id[disk]
        cmd = ['hwmgr', 'show', 'scsi', '-id', id, '-full']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return
        for line in out.split('\n'):
            v = line.split()
            if len(v) != 4:
                continue
            data = {}
            for i, s in enumerate(("bus", "target", "lun")):
                try:
                    j = int(v[i])
                    data[s] = v[i]
                except:
                    continue
            self.itn[disk].append(data)

    def set_nexus(self, itn):
        return "set nexus bus %(bus)s target %(target)s lun %(lun)s ; " % itn

    def pipe_scu(self, cmd):
        self.log.info(cmd + ' | scu')
        p = Popen(['scu'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate(input=cmd)
        # if len(out) > 0:
        #    self.log.info(out)
        if len(err) > 0:
            self.log.error(out)
        if p.returncode:
            raise ex.Error

    def disk_registered(self, disk):
        cmd = ['scu', '-f', disk, 'show', 'keys']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if out.count(self.hostid) == 0:
            return False
        return True

    def disk_register(self, disk):
        self.get_itns()
        basedisk = os.path.basename(disk)
        if basedisk not in self.itn:
            self.log.error("no nexus information for disk %s" % disk)
            return 1
        r = 0
        for itn in self.itn[basedisk]:
            r += self.__disk_register(itn)
        if r > 0:
            r = 1
        return r

    def __disk_register(self, itn):
        cmd = self.set_nexus(itn) + 'preserve register skey ' + self.hostid
        try:
            self.pipe_scu(cmd)
        except ex.Error as e:
            self.log.error("failed to register key %s with nexus %s" % (self.hostid, ':'.join(itn.values())))
            return 1
        return 0

    def disk_unregister(self, disk):
        self.get_itns()
        basedisk = os.path.basename(disk)
        if basedisk not in self.itn:
            self.log.error("no nexus information for disk %s" % disk)
            return 1
        r = 0
        for itn in self.itn[basedisk]:
            r += self.__disk_unregister(itn)
        if r > 0:
            r = 1
        return r

    def __disk_unregister(self, itn):
        cmd = self.set_nexus(itn) + 'preserve register skey 0 key ' + self.hostid
        try:
            self.pipe_scu(cmd)
        except ex.Error as e:
            self.log.error("failed to unregister key %s with nexus %s" % (self.hostid, ':'.join(itn.values())))
            return 1
        return 0

    def get_reservation_key(self, disk):
        cmd = ['scu', '-f', disk, 'show', 'reservation']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to list reservation for disk %s" % disk)
        if 'Reservation Key' not in out:
            return None
        for line in out.split('\n'):
            if 'Reservation Key' in line:
                return line.split()[-1]
        raise Exception()

    def disk_reserved(self, disk):
        cmd = ['scu', '-f', disk, 'show', 'reservation']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read reservation for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_release(self, disk):
        cmd = ['scu', '-f', disk, 'preserve', 'release', 'key', self.hostid, 'type', self.prtype]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to release disk %s" % disk)
        return ret

    def disk_reserve(self, disk):
        cmd = ['scu', '-f', disk, 'preserve', 'reserve', 'key', self.hostid, 'type', self.prtype]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to reserve disk %s" % disk)
        return ret

    def _disk_preempt_reservation(self, disk, oldkey):
        cmd = ['scu', '-f', disk, 'preserve', 'preempt', 'key', self.hostid, 'skey', oldkey, 'type', self.prtype]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to preempt reservation for disk %s" % disk)
        return ret
