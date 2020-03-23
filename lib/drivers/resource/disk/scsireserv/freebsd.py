from .sg import DiskScsireservSg
from rcUtilitiesLinux import dev_to_paths

class Scsireserv(DiskScsireservSg):
    def mangle_devs(self, devs):
        return dict((dev, dev_to_paths(dev)) for dev in devs)
