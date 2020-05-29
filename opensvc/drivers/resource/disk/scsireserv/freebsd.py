import utilities.devices.linux

from .sg import DiskScsireservSg

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "scsireserv"

class DiskScsireserv(DiskScsireservSg):
    def mangle_devs(self, devs):
        return dict((dev, utilities.devices.linux.dev_to_paths(dev)) for dev in devs)
