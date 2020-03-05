import resScsiReservSg
from rcUtilitiesLinux import dev_to_paths

class ScsiReserv(resScsiReservSg.ScsiReserv):
    def mangle_devs(self, devs):
        return dict((dev, dev_to_paths(dev)) for dev in devs)
