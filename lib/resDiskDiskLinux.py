from __future__ import print_function

import glob
import os
import time

import rcExceptions as ex
import resDiskDisk
from rcUtilities import lazy, which, justcall
from rcUtilitiesLinux import multipath_flush, dev_delete, dev_to_paths
from rcGlobalEnv import rcEnv
import rcStatus

class Disk(resDiskDisk.Disk):
    @lazy
    def devpath(self):
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

    def _status(self, verbose=False):
        if self.disk_id is None:
            return rcStatus.NA
        if not self.devpath or not os.path.exists(self.devpath):
            self.status_log("%s does not exist" % self.devpath, "warn")
            return rcStatus.DOWN
        return rcStatus.NA

    def exposed_devs(self):
        try:
            dev = os.path.realpath(self.devpath)
            return set([dev])
        except Exception as exc:
            pass
        return set()

    def unprovision(self):
        # executed on all nodes
        try:
            mpath = list(self.exposed_devs())[0] # /dev/dm-<minor>
        except IndexError:
            mpath = None
        if mpath and mpath.startswith("/dev/dm-"):
            paths = dev_to_paths(mpath)
            multipath_flush(mpath, log=self.log)
            for path in paths:
                dev_delete(path, log=self.log)
        self.svc.node.unset_lazy("devtree")

        # executed on the leader only
        resDiskDisk.Disk.unprovision(self)

    def provision(self):
        # executed on the leader only
        resDiskDisk.Disk.provision(self)

        # executed on all nodes
        self.unset_lazy("disk_id")
        self.unset_lazy("anypath")
        self.unset_lazy("devpath")
        if not self.disk_id:
            raise ex.excError("disk_id is not set. should be at this point")
        self.svc.node._scanscsi()
        self.wait_udev()
        self.unset_lazy("devpath")
        self.svc.node.unset_lazy("devtree")
        if self.devpath and which(rcEnv.syspaths.multipath):
            dev = os.path.realpath(self.devpath)
            cmd = [rcEnv.syspaths.multipath, "-v1", dev]
            ret, out, err = self.vcall(cmd)

    def wait_udev(self):
        for retry in range(30):
            if os.path.exists(self.anypath):
                self.log.info("%s now exists", self.anypath)
                return
            time.sleep(1)
        raise ex.excError("time out waiting for %s to appear" % self.anypath)

