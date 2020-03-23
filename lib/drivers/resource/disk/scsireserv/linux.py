import rcStatus

from .sg import DiskScsireservSg
from rcUtilitiesLinux import dev_to_paths, dev_is_ro

class Scsireserv(DiskScsireservSg):


    def mangle_devs(self, devs):
        return dict((dev, dev_to_paths(dev)) for dev in devs)


    def _status(self, verbose=False):
        ret = super()._status(verbose=verbose)
        if ret != rcStatus.UP:
            return ret
        self.get_devs()
        for dev, paths in self.devs.items():
            for path in paths:
                if dev_is_ro(path):
                    self.status_log("resv held on ro dev %s in %s" % (path, dev), "info")
                    return rcStatus.NA
        return ret
