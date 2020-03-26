import os
import time

import core.exceptions as ex
import core.status
import utilities.devices.sunos

from . import \
    BaseDiskLoop, \
    adder as base_loop_adder, \
    KEYWORDS, \
    DRIVER_GROUP, \
    DRIVER_BASENAME, \
    DEPRECATED_SECTIONS
from converters import convert_size
from lock import cmlock
from rcGlobalEnv import rcEnv
from core.objects.svcdict import KEYS

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
        self.loop = utilities.devices.sunos.file_to_loop(self.loopFile)
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
                cmd = ['lofiadm', '-a', self.loopFile]
                ret, out, err = self.vcall(cmd)
        except Exception as exc:
            raise ex.Error(str(exc))
        if ret != 0:
            raise ex.Error
        self.loop = utilities.devices.sunos.file_to_loop(self.loopFile)
        if len(self.loop) == 0:
            raise ex.Error("loop device did not appear or disappeared")
        time.sleep(1)
        self.log.info("%s now loops to %s" % (self.loop, self.loopFile))
        self.can_rollback = True

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.label)
            return 0
        cmd = ['lofiadm', '-d', self.loop]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error

    def resource_handling_file(self):
        path = os.path.dirname(self.loopFile)
        return self.svc.resource_handling_dir(path)

    def _status(self, verbose=False):
        r = self.resource_handling_file()
        if self.is_provisioned() and not os.path.exists(self.loopFile):
            if r is None or (r and r.status() in (core.status.UP, core.status.STDBY_UP)):
                self.status_log("%s does not exist" % self.loopFile)
        if self.is_up():
            return core.status.UP
        else:
            return core.status.DOWN

    def exposed_devs(self):
        self.loop = utilities.devices.sunos.file_to_loop(self.loopFile)
        return set(self.loop)

    def provisioned(self):
        try:
            return os.path.exists(self.loopFile)
        except Exception:
            return

    def unprovisioner(self):
        try:
            self.loopFile
        except Exception as e:
            raise ex.Error(str(e))

        if not self.provisioned():
            return

        self.log.info("unlink %s" % self.loopFile)
        os.unlink(self.loopFile)
        self.svc.node.unset_lazy("devtree")

    def provisioner(self):
        self.size = self.oget("size")
        d = os.path.dirname(self.loopFile)
        try:
            if not os.path.exists(d):
                self.log.info("create directory %s"%d)
                os.makedirs(d)
            with open(self.loopFile, 'w') as f:
                self.log.info("create file %s, size %s"%(self.loopFile, self.size))
                f.seek(convert_size(self.size, _to='b', _round=512)-1)
                f.write('\0')
        except Exception as e:
            raise ex.Error("failed to create %s: %s"% (self.loopFile, str(e)))
        self.svc.node.unset_lazy("devtree")
