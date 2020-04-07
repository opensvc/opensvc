import os
from subprocess import *

import core.exceptions as ex
from utilities.proc import which
from . import BaseDiskScsireserv

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "scsireserv"


def mpath_to_path(disks):
    l = []
    for disk in disks:
        if "/dev/dsk" in disk:
            l.append(disk.replace("/dev/dsk", "/dev/rdsk"))
            continue
        if "/dev/disk" not in disk and "/dev/rdisk" not in disk:
            continue
        if not os.path.exists(disk):
            continue
        cmd = ['ioscan', '-F', '-m', 'dsf', disk]
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        buff = p.communicate()
        ret = p.returncode
        if ret != 0:
            continue
        a = buff[0].split(':')
        if len(a) != 2:
            continue
        b = a[1].split()
        for d in b:
            l.append(d.replace("/dev/dsk", "/dev/rdsk"))
    return l


class DiskScsireserv(BaseDiskScsireserv):
    def __init__(self, **kwargs):
        super(DiskScsireserv, self).__init__(**kwargs)
        self.leg_mpath_disable()

    def get_devs(self):
        self.devs = mpath_to_path(self.peer_resource.base_devs())

    def scsireserv_supported(self):
        if which('scu') is None:
            return False
        return True

    def leg_mpath_disable(self):
        cmd = ['scsimgr', 'get_attr', '-p', '-a', 'leg_mpath_enable']
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        buff = p.communicate()
        ret = p.returncode
        if ret != 0:
            self.log.error("can not fetch 'leg_mpath_enable' value")
            raise ex.Error
        if 'false' in buff[0]:
            return
        cmd = ['scsimgr', 'save_attr', '-a', 'leg_mpath_enable=false']
        self.log.info(' '.join(cmd))
        p = Popen(cmd, stderr=None, stdout=PIPE, close_fds=True)
        p.communicate()
        ret = p.returncode
        if ret != 0:
            self.log.error("can not set 'leg_mpath_enable' value")
            raise ex.Error

    def ack_unit_attention(self, d):
        return 0

    def disk_registered(self, disk):
        cmd = ['scu', '-f', disk, 'show', 'keys']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            self.log.error("failed to read registrations for disk %s" % disk)
        if self.hostid in out:
            return True
        return False

    def disk_register(self, disk):
        cmd = ['scu', '-f', disk, 'preserve', 'register', 'skey', self.hostid]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to register key %s with disk %s" % (self.hostid, disk))
        return ret

    def disk_unregister(self, disk):
        cmd = ['scu', '-f', disk, 'preserve', 'register', 'skey', '0', 'key', self.hostid]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            self.log.error("failed to unregister key %s with disk %s" % (self.hostid, disk))
        return ret

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
