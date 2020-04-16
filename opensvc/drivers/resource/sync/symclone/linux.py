import time

import core.exceptions as ex

from . import \
    SyncSymclone as BaseSyncSymclone, \
    KEYWORDS, \
    DRIVER_GROUP, \
    DRIVER_BASENAME, \
    driver_capabilities
from env import Env
from core.objects.svcdict import KEYS
from utilities.proc import which

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class SyncSymclone(BaseSyncSymclone):
    def __init__(self,
                 type="sync.symclone",
                 symid=None,
                 pairs=None,
                 precopy=True,
                 consistent=True,
                 **kwargs):
        super(SyncSymclone, self).__init__(type=type, **kwargs)
        if pairs is None:
            pairs = []

    def dev_rescan(self, dev):
        dev = dev.replace('/dev/', '')
        sysdev = "/sys/block/%s/device/rescan"%dev
        self.log.info("echo 1>%s"%sysdev)
        with open(sysdev, 'w') as s:
            s.write("1")

    def refresh_multipath(self, dev):
        if which(Env.syspaths.multipath) is None:
            return
        cmd = [Env.syspaths.multipath, '-v0', '-r', dev]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

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
        raise ex.Error

    def wait_for_devs_ready(self):
        for pair in self.pairs:
            src, dst = self.split_pair(pair)
            dev = self.showdevs_etree[dst].find('Dev_Info/pd_name').text
            if "Not Visible" in dev:
                raise ex.Error("pd name is 'Not Visible'. please scan scsi buses and run symcfg discover")
            self.dev_rescan(dev)
            self.wait_for_dev_ready(dev)
            self.refresh_multipath(dev)


