from .sg import DiskScsireservSg, driver_capabilities

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "scsireserv"
assert driver_capabilities  # ensure driver can scan its capabilities


class DiskScsireserv(DiskScsireservSg):
    pass
