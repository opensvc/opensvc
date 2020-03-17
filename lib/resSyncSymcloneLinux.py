import os
import time

import rcExceptions as ex
import resSyncSymclone

from rcGlobalEnv import rcEnv
from rcUtilities import which
from svcBuilder import sync_kwargs


def adder(svc, s, drv=None, t="sync.symclone"):
    drv = drv or SyncSymclone
    kwargs = {}
    kwargs["type"] = t
    kwargs["pairs"] = svc.oget(s, "pairs")
    kwargs["symid"] = svc.oget(s, "symid")
    kwargs["recreate_timeout"] = svc.oget(s, "recreate_timeout")
    kwargs["restore_timeout"] = svc.oget(s, "restore_timeout")
    kwargs["consistent"] = svc.oget(s, "consistent")
    kwargs["precopy"] = svc.oget(s, "precopy")
    kwargs.update(sync_kwargs(svc, s))
    r = drv(**kwargs)
    svc += r


class SyncSymclone(resSyncSymclone.SyncSymclone):
    def __init__(self,
                 rid=None,
                 type="sync.symclone",
                 symid=None,
                 pairs=[],
                 precopy=True,
                 consistent=True,
                 **kwargs):
        resSyncSymclone.SyncSymclone.__init__(self,
                                       rid=rid,
                                       type=type,
                                       symid=symid,
                                       pairs=pairs,
                                       precopy=precopy,
                                       consistent=consistent,
                                       **kwargs)

    def dev_rescan(self, dev):
        dev = dev.replace('/dev/', '')
        sysdev = "/sys/block/%s/device/rescan"%dev
        self.log.info("echo 1>%s"%sysdev)
        with open(sysdev, 'w') as s:
            s.write("1")

    def refresh_multipath(self, dev):
        if which(rcEnv.syspaths.multipath) is None:
            return
        cmd = [rcEnv.syspaths.multipath, '-v0', '-r', dev]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def dev_ready(self, dev):
        cmd = ['sg_turs', dev]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            return False
        return True

    def wait_for_dev_ready(self, dev):
        delay = 1
        timeout = 5
        for i in range(timeout/delay):
            if self.dev_ready(dev):
                return
            if i == 0:
                self.log.info("waiting for device %s to become ready (max %i secs)"%(dev,timeout))
            time.sleep(delay)
        self.log.error("timed out waiting for device %s to become ready (max %i secs)"%(dev,timeout))
        raise ex.excError

    def wait_for_devs_ready(self):
        for pair in self.pairs:
            src, dst = self.split_pair(pair)
            dev = self.showdevs_etree[dst].find('Dev_Info/pd_name').text
            if "Not Visible" in dev:
                raise ex.excError("pd name is 'Not Visible'. please scan scsi buses and run symcfg discover")
            self.dev_rescan(dev)
            self.wait_for_dev_ready(dev)
            self.refresh_multipath(dev)

