"""
Base disk resource driver module.
"""

import os
import resources as Res
import rcStatus
import rcExceptions as exc
from rcGlobalEnv import rcEnv

class Disk(Res.Resource):
    """
    Base disk resource driver, derived for LVM, Veritas, ZFS, ...
    """
    def __init__(self, rid=None, name=None, **kwargs):
        Res.Resource.__init__(self, rid, **kwargs)
        self.name = name
        self.disks = set()
        self.devs = set()

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def disklist(self):
        return self.disks

    def has_it(self): return False
    def is_up(self): return False
    def do_start(self): return False
    def do_stop(self): return False

    def stop(self):
        self.do_stop()

    def start(self):
        self.do_start()

    def _status(self, verbose=False):
        if self.is_up():
            state = rcStatus.UP
        else:
            state = rcStatus.DOWN
        return self.status_stdby(state)

    def create_static_name(self, dev, suffix="0"):
        d = self.create_dev_dir()
        lname = self.rid.replace("#", ".") + "." + suffix
        l = os.path.join(d, lname)
        if os.path.exists(l) and os.path.realpath(l) == dev:
            return
        self.log.info("create static device name %s -> %s" % (l, dev))
        try:
            os.unlink(l)
        except:
            pass
        os.symlink(dev, l)

    def create_dev_dir(self):
        d = os.path.join(rcEnv.paths.pathvar, self.svc.svcname, "dev")
        if os.path.exists(d):
            return d
        os.makedirs(d)
        return d

if __name__ == "__main__":
    for c in (Disk,) :
        help(c)

    print("""d=Disk("aGenericDisk")""")
    d=Disk("aGenericDisk")
    print("show d", d)
    print("""d.do_action("start")""")
    d.do_action("start")
    print("""d.do_action("stop")""")
    d.do_action("stop")
