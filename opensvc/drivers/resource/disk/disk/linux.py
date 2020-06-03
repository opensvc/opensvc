import glob
import os
import time

import core.exceptions as ex
import core.status
import utilities.devices.linux

from . import DiskDisk as BaseDiskDisk, KEYWORDS
from utilities.lazy import lazy
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import which

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "disk"

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class DiskDisk(BaseDiskDisk):
    @lazy
    def devpath(self):
        self.unset_lazy("disk_id")
        wwid = str(self.disk_id).lower().replace("0x", "")
        try:
            return glob.glob("/dev/disk/by-id/dm-uuid-mpath-[36]%s" % wwid)[0]
        except Exception as exc:
            return

    @lazy
    def anypath(self):
        wwid = str(self.disk_id).lower().replace("0x", "")
        path = "/dev/disk/by-id/wwn-0x%s" % wwid
        return path

    def sub_devs(self):
        if self.devpath:
            return set([self.devpath])
        else:
            return set()

    def _status(self, verbose=False):
        if self.disk_id is None:
            return core.status.NA
        if not self.devpath or not os.path.exists(self.devpath):
            if self.devpath:
                self.status_log("%s does not exist" % self.devpath, "warn")
            return core.status.DOWN
        return core.status.NA

    def exposed_devs(self):
        self.unset_lazy("devpath")
        try:
            dev = os.path.realpath(self.devpath)
            return set([dev])
        except Exception as exc:
            pass
        return set()

    def unconfigure(self):
        self.log.info("unconfigure disk %s", self.disk_id)
        try:
            mpath = list(self.exposed_devs())[0] # /dev/dm-<minor>
        except IndexError:
            mpath = None
        if mpath and mpath.startswith("/dev/dm-"):
            paths = utilities.devices.linux.dev_to_paths(mpath)
            utilities.devices.linux.multipath_flush(mpath, log=self.log)
            for path in paths:
                utilities.devices.linux.dev_delete(path, log=self.log)
        self.svc.node.unset_lazy("devtree")

    def configure(self, force=False):
        self.unset_lazy("disk_id")
        self.unset_lazy("anypath")
        self.unset_lazy("devpath")
        if not force and self.exposed_devs():
            self.log.info("disk already configured: exposed devs %s", self.exposed_devs())
            return
        self.log.info("configure disk %s", self.disk_id)
        if not self.disk_id:
            raise ex.Error("disk_id is not set. should be at this point")
        self.svc.node._scanscsi(log=self.log)
        self.wait_anypath()
        self.svc.node.unset_lazy("devtree")
        if self.devpath and which(Env.syspaths.multipath):
            dev = os.path.realpath(self.devpath)
            cmd = [Env.syspaths.multipath, "-v1", dev]
            ret, out, err = self.vcall(cmd)
        self.wait_devpath()

    def wait_anypath(self):
        for retry in range(30):
            self.unset_lazy("anypath")
            if self.anypath and os.path.exists(self.anypath):
                self.log.info("%s now exists", self.anypath)
                return
            time.sleep(1)
        raise ex.Error("time out waiting for %s to appear" % self.anypath)

    def wait_devpath(self):
        for retry in range(30):
            self.unset_lazy("devpath")
            if self.devpath and os.path.exists(self.devpath):
                self.log.info("%s now exists", self.devpath)
                return
            time.sleep(1)
        raise ex.Error("time out waiting for %s to appear" % self.devpath)

