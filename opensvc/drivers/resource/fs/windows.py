import os

import core.exceptions as ex
from utilities.lazy import lazy
from utilities.mounts.windows import Mounts
from . import BaseFs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = ""

def diskpartfile_name(self):
    return os.path.join(self.var_d, 'diskpart')

def online_drive(self, driveindex):
    diskpart_file = diskpartfile_name(self) + '_online_disk_' + str(driveindex)
    with open(diskpart_file, 'w') as f:
        f.write("select disk=%s\n"%driveindex)
        f.write("online disk\n")
        f.write("exit\n")
    self.log.info("bring disk %s online"%driveindex)
    cmd = ['diskpart', '/s', diskpart_file]
    (ret, out, err) = self.vcall(cmd)
    if ret != 0:
        raise ex.Error("Failed to run command %s"% ' '.join(cmd) )

def offline_drive(self, driveindex):
    diskpart_file = diskpartfile_name(self) + '_offline_disk_' + str(driveindex)
    with open(diskpart_file, 'w') as f:
        f.write("select disk=%s\n"%driveindex)
        f.write("offline disk\n")
        f.write("exit\n")
    self.log.info("bring disk %s offline", driveindex)
    cmd = ['diskpart', '/s', diskpart_file]
    (ret, out, err) = self.vcall(cmd)
    if ret != 0:
        raise ex.Error("Failed to run command %s"% ' '.join(cmd) )

class Fs(BaseFs):
    """
    The Windows fs class
    """

    @lazy
    def drive(self):
        return self.mount_point.split(":", 1)[0] + ":"

    @lazy
    def volume(self):
        vols = self.svc.node.wmi().Win32_Volume()
        for vol in vols:
            if vol.DeviceId == self.device_id:
                return vol
        raise ex.Error("volume %s not found" % self.device)

    @lazy
    def device_id(self):
        if os.sep not in self.device:
            return os.sep+os.sep+os.path.join("?", "Volume{%s}" % self.device, "")
        else:
            return self.device

    def mount(self):
        ret = 0
        changed = False

        if self.volume.DriveLetter == self.drive:
            self.log.info("drive %s already assigned", self.drive)
        else:
            self.log.info("assign drive %s", self.drive)
            self.volume.DriveLetter = self.drive
            changed = True

        if not self.volume.Automount:
            self.log.info("mount volume %s", self.device)
            ret, = self.volume.Mount()
            changed = True

        if changed:
            self.can_rollback = True
            self.unset_lazy("volume")

        return ret

    def try_umount(self):
        if self.volume.DriveLetter is None:
            self.log.info("drive %s already unassigned", self.drive)
            return 0
        self.log.info("unassign drive %s", self.drive)
        self.volume.DriveLetter = None
        self.unset_lazy("volume")
        if self.volume.DriveLetter is None:
            return 0
        return 1

    # a completer
    def match_mount(self, i, dev, mnt):
        return True

    # a completer
    def is_online(self):
        return True

    def is_up(self):
        return Mounts(wmi=self.svc.node.wmi()).has_mount(self.device_id, self.mount_point)

    def start_mount(self):
        if self.is_online():
            self.log.info("%s is already online", self.device)
        if self.is_up():
            self.log.info("%s is already mounted", self.device)
            return 0
        ret = self.mount()
        if ret != 0:
            return 1
        return 0

    def stop(self):
        if self.is_up() is False:
            self.log.info("%s is already umounted", self.device)
            return
        ret = self.try_umount()
        if ret != 0:
            raise ex.Error("failed to umount %s" % self.device)
        return 0
