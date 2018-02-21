import resScsiReservSg
from rcUtilitiesLinux import dev_to_paths

class ScsiReserv(resScsiReservSg.ScsiReserv):
    def mangle_devs(self, devs):
        _devs = set()
        for dev in devs:
            _devs |= set(dev_to_paths(dev))
        return _devs
