import os
import time

import core.exceptions as ex
import core.status
import utilities.devices.sunos

from . import BaseDiskLoop
from utilities.converters import convert_size
from utilities.lock import cmlock
from env import Env
from core.objects.svcdict import KEYS

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "loop"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("lofiadm"):
        return ["disk.loop"]
    return []


class DiskLoop(BaseDiskLoop):
    def is_up(self):
        """Returns True if the loop group is present and activated
        """
        self.loop = utilities.devices.sunos.file_to_loop(self.loopfile)
        if len(self.loop) == 0:
            return False
        return True

    def start(self):
        lockfile = os.path.join(Env.paths.pathlock, "disk.loop")
        if self.is_up():
            self.log.info("%s is already up" % self.label)
            return
        try:
            with cmlock(timeout=30, delay=1, lockfile=lockfile):
                cmd = ['lofiadm', '-a', self.loopfile]
                ret, out, err = self.vcall(cmd)
        except Exception as exc:
            raise ex.Error(str(exc))
        if ret != 0:
            raise ex.Error
        self.loop = utilities.devices.sunos.file_to_loop(self.loopfile)
        if len(self.loop) == 0:
            raise ex.Error("loop device did not appear or disappeared")
        time.sleep(1)
        self.log.info("%s now loops to %s" % (self.loop, self.loopfile))
        self.can_rollback = True

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return 0
        for loop in self.loop:
            cmd = ['lofiadm', '-d', loop]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

    def resource_handling_file(self):
        path = os.path.dirname(self.loopfile)
        return self.svc.resource_handling_dir(path)

    def _status(self, verbose=False):
        r = self.resource_handling_file()
        if self.is_provisioned() and not os.path.exists(self.loopfile):
            if r is None or (r and r.status() in (core.status.UP, core.status.STDBY_UP)):
                self.status_log("%s does not exist" % self.loopfile)
        if self.is_up():
            return core.status.UP
        else:
            return core.status.DOWN

    def exposed_devs(self):
        self.loop = utilities.devices.sunos.file_to_loop(self.loopfile)
        return set(self.loop)

    def provisioned(self):
        try:
            return os.path.exists(self.loopfile)
        except Exception:
            return

    def unprovisioner(self):
        try:
            self.loopfile
        except Exception as e:
            raise ex.Error(str(e))

        if not self.provisioned():
            return

        self.log.info("unlink %s" % self.loopfile)
        os.unlink(self.loopfile)
        self.svc.node.unset_lazy("devtree")

    def provisioner(self):
        d = os.path.dirname(self.loopfile)
        try:
            if not os.path.exists(d):
                self.log.info("create directory %s"%d)
                os.makedirs(d)
            with open(self.loopfile, 'w') as f:
                self.log.info("create file %s, size %s"%(self.loopfile, self.size))
                f.seek(convert_size(self.size, _to='b', _round=512)-1)
                f.write('\0')
            self.chown()
            self.chmod()
        except Exception as e:
            raise ex.Error("failed to create %s: %s"% (self.loopfile, str(e)))
        self.svc.node.unset_lazy("devtree")
