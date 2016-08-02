import os
import re

from rcGlobalEnv import *
from rcUtilities import call, which
import rcStatus
import resDiskLoop as Res
import rcExceptions as ex
from rcDiskLoopDarwin import file_to_loop

class Disk(Res.Disk):
    def is_up(self):
        """Returns True if the loop group is present and activated
        """
        self.loop = file_to_loop(self.loopFile)
        if len(self.loop) == 0:
            return False
        return True

    def start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.loopFile)
            return
        cmd = ['hdiutil', 'attach', self.loopFile]
        (ret, out, err) = self.call(cmd, info=True, outlog=False)
        if ret != 0:
            raise ex.excError
        self.loop = file_to_loop(self.loopFile)
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
                raise ex.excError

    def _status(self, verbose=False):
        if self.is_up():
            return self.status_stdby(rcStatus.UP)
        else:
            return self.status_stdby(rcStatus.DOWN)

    def __init__(self,
                 rid,
                 loopFile,
                 always_on=set([]),
                 disabled=False,
                 tags=set([]),
                 optional=False,
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Disk.__init__(self,
                          rid,
                          loopFile,
                          always_on=always_on,
                          disabled=disabled,
                          tags=tags,
                          optional=optional,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

