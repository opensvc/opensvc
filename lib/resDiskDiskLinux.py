from __future__ import print_function

import os

import rcExceptions as ex
import resDiskDisk
from rcUtilities import lazy
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
            dev = os.path.realpath(os.path.join(os.path.dirname(self.devpath), os.readlink(self.devpath)))
            return set([dev])
        except Exception as exc:
            print(exc)
            pass
        return set()
