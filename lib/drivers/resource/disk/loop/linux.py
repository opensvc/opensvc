import os
import re
import time

import rcExceptions as ex
import rcStatus

from . import \
    BaseDiskLoop, \
    adder as base_loop_adder, \
    KEYWORDS, \
    DRIVER_GROUP, \
    DRIVER_BASENAME, \
    DEPRECATED_SECTIONS
from lock import cmlock
from rcGlobalEnv import rcEnv
from rcLoopLinux import file_to_loop
from rcUtilities import call, which, clear_cache
from svcdict import KEYS

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)

def adder(svc, s):
    base_loop_adder(svc, s, drv=DiskLoop)


class DiskLoop(BaseDiskLoop):
    def is_up(self):
        """Returns True if the loop group is present and activated
        """
        self.loop = file_to_loop(self.loopFile)
        if len(self.loop) == 0:
            return False
        return True

    def start(self):
        lockfile = os.path.join(rcEnv.paths.pathlock, "disk.loop")
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return
        try:
            with cmlock(timeout=30, delay=1, lockfile=lockfile):
                cmd = [rcEnv.syspaths.losetup, '-f', self.loopFile]
                ret, out, err = self.vcall(cmd)
                clear_cache("losetup.json")
        except Exception as exc:
            raise ex.excError(str(exc))
        if ret != 0:
            raise ex.excError
        self.loop = file_to_loop(self.loopFile)
        if len(self.loop) == 0:
            raise ex.excError("loop device did not appear or disappeared")
        time.sleep(2)
        self.log.info("%s now loops to %s" % (', '.join(self.loop), self.loopFile))
        self.can_rollback = True

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return 0
        for loop in self.loop:
            cmd = [rcEnv.syspaths.losetup, '-d', loop]
            ret, out, err = self.vcall(cmd)
            clear_cache("losetup.json")
            if ret != 0:
                raise ex.excError

    def resource_handling_file(self):
        path = os.path.dirname(self.loopFile)
        return self.svc.resource_handling_dir(path)

    def _status(self, verbose=False):
        r = self.resource_handling_file()
        if self.is_provisioned() and not os.path.exists(self.loopFile):
            if r is None or (r and r.status() in (rcStatus.UP, rcStatus.STDBY_UP)):
                self.status_log("%s does not exist" % self.loopFile)
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def __init__(self, rid, loopFile, **kwargs):
        super().__init__(rid, loopFile, **kwargs)

    def exposed_devs(self):
        self.loop = file_to_loop(self.loopFile)
        return set(self.loop)

