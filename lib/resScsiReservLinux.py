import resScsiReservSg
import rcStatus
from rcUtilitiesLinux import dev_to_paths, dev_is_ro

class ScsiReserv(resScsiReservSg.ScsiReserv):
    def mangle_devs(self, devs):
        _devs = set()
        for dev in devs:
            _devs |= set(dev_to_paths(dev))
        return _devs

    def _status(self, verbose=False):
        ret = resScsiReservSg.ScsiReserv._status(self, verbose=verbose)
        if ret != rcStatus.UP:
            return ret
        self.get_devs()
        for dev in self.devs:
            if dev_is_ro(dev):
                self.status_log("resv held on ro dev %s" % dev, "info")
                return rcStatus.NA
        return ret
