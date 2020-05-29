import core.status
import utilities.devices.linux

from .sg import DiskScsireservSg, driver_capabilities

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "scsireserv"


class DiskScsireserv(DiskScsireservSg):
    def mangle_devs(self, devs):
        return dict((dev, utilities.devices.linux.dev_to_paths(dev)) for dev in devs)

    def _status(self, verbose=False):
        ret = super(DiskScsireserv, self)._status(verbose=verbose)
        if ret != core.status.UP:
            return ret
        self.get_devs()
        for dev, paths in self.devs.items():
            for path in paths:
                if utilities.devices.linux.dev_is_ro(path):
                    self.status_log("resv held on ro dev %s in %s" % (path, dev), "info")
                    return core.status.NA
        return ret
