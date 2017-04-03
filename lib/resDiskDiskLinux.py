from __future__ import print_function

import os
import time

import rcExceptions as ex
import resDiskDisk
from rcUtilities import lazy, which, justcall
import rcStatus

class Disk(resDiskDisk.Disk):
    @lazy
    def devpath(self):
        return "/dev/disk/by-id/wwn-0x%s" % str(self.disk_id).lower().replace("0x", "")

    def _status(self, verbose=False):
        if self.disk_id is None:
            return rcStatus.NA
        if not os.path.exists(self.devpath):
            self.status_log("%s does not exist" % self.devpath, "warn")
            return rcStatus.DOWN
        return rcStatus.NA

    def devlist(self):
        try:
            dev = os.path.realpath(self.devpath)
            return set([dev])
        except Exception as exc:
            print(exc)
            pass
        return set()

    def provision(self):
        resDiskDisk.Disk.provision(self)
        self.wait_udev()
        if which("multipath"):
            dev = os.path.realpath(self.devpath)
            cmd = ["multipath", "-v1", dev]
            ret, out, err = self.vcall(cmd)

    def wait_udev(self):
        for retry in range(30):
            if os.path.exists(self.devpath):
                self.log.info("%s now exists", self.devpath)
                return
            time.sleep(1)
        raise ex.excError("time out waiting for %s to appear" % self.devpath)

