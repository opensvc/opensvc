import os
import re
import time

from rcGlobalEnv import *
from rcUtilities import call, which
import rcStatus
import resDiskLoop as Res
import rcExceptions as ex
from rcLoopLinux import file_to_loop
from lock import cmlock

class Disk(Res.Disk):
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
                (ret, out, err) = self.vcall(cmd)
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
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.excError

    def parent_dir_handled_by_service(self):
        d = os.path.dirname(self.loopFile)
        mntpts = {}
        for r in self.svc.get_resources(["fs"]):
            mntpts[r.mount_point] = r
        while True:
            if d in mntpts.keys():
                return mntpts[d]
            d = os.path.dirname(d)
            if d == os.sep:
                return

    def _status(self, verbose=False):
        r = self.parent_dir_handled_by_service()
        if not os.path.exists(self.loopFile):
            if r is None or (r and r.status() in (rcStatus.UP, rcStatus.STDBY_UP)):
                self.status_log("%s does not exist" % self.loopFile)
        if self.is_up():
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def __init__(self, rid, loopFile, **kwargs):
        Res.Disk.__init__(self, rid, loopFile, **kwargs)

    def exposed_devs(self):
        self.loop = file_to_loop(self.loopFile)
        return set(self.loop)

