import os

import core.status
import core.exceptions as ex
import utilities.devices.darwin

from . import \
    BaseDiskLoop, \
    adder as base_loop_adder, \
    KEYWORDS, \
    DRIVER_GROUP, \
    DRIVER_BASENAME, \
    DEPRECATED_SECTIONS
from utilities.converters import convert_size
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
        self.loop = utilities.devices.darwin.file_to_loop(self.loopFile)
        if len(self.loop) == 0:
            return False
        return True

    def start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.loopFile)
            return
        cmd = ['hdiutil', 'attach', '-imagekey', 'diskimage-class=CRawDiskImage', '-nomount', self.loopFile]
        (ret, out, err) = self.call(cmd, info=True, outlog=False)
        if ret != 0:
            raise ex.Error
        self.loop = utilities.devices.darwin.file_to_loop(self.loopFile)
        self.log.info("%s now loops to %s" % (', '.join(self.loop), self.loopFile))
        self.can_rollback = True

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.loopFile)
            return 0
        for loop in self.loop:
            cmd = ['hdiutil', 'detach', loop.strip('md')]
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

    def _status(self, verbose=False):
        if self.is_up():
            return core.status.UP
        else:
            return core.status.DOWN

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