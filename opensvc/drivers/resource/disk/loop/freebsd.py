import os

import core.exceptions as ex
import core.status
from core.objects.svcdict import KEYS
from utilities.proc import call, which
from . import BaseDiskLoop

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "loop"

def driver_capabilities(node=None):
    from utilities.proc import which
    if which("mdconfig"):
        return ["disk.loop"]
    return []


def file_to_loop(f):
    """Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/loop0
    """
    if which('mdconfig') is None:
        return []
    if not os.path.isfile(f):
        return []
    (ret, out, err) = call(['mdconfig', '-l', '-v'])
    if ret != 0:
        return []
    """ It's possible multiple loopdev are associated with the same file
    """
    devs= []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 4:
            continue
        path = ' '.join(l[3:])
        if path != f:
            continue
        if not os.path.exists('/dev/'+l[0]):
            continue
        devs.append(l[0])
    return devs

class DiskLoop(BaseDiskLoop):
    def is_up(self):
        """Returns True if the loop group is present and activated
        """
        self.loop = file_to_loop(self.loopfile)
        if len(self.loop) == 0:
            return False
        return True

    def start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.loopfile)
            return
        cmd = ['mdconfig', '-a', '-t', 'vnode', '-f', self.loopfile]
        (ret, out, err) = self.call(cmd, info=True, outlog=False)
        if ret != 0:
            raise ex.Error
        self.loop = file_to_loop(self.loopfile)
        self.log.info("%s now loops to %s" % (', '.join(self.loop), self.loopfile))
        self.can_rollback = True

    def stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.loopfile)
            return 0
        for loop in self.loop:
            cmd = ['mdconfig', '-d', '-u', loop.strip('md')]
            (ret, out, err) = self.vcall(cmd)
            if ret != 0:
                raise ex.Error

    def _status(self, verbose=False):
        if self.is_up():
            return core.status.UP
        else:
            return core.status.DOWN
