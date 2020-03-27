import core.exceptions as ex

class BaseDiskInfo(object):
    """
    Parent class for diskInfo OS
    """

    print_diskinfo_fmt = "%-12s %-8s %12s MB %-8s %-8s %-16s"

    def disk_id(self, dev):
        return "tbd"


    def disk_vendor(self, dev):
        return "tbd"


    def disk_model(self, dev):
        return "tbd"


    def disk_size(self, dev):
        return "tbd"


    def diskinfo_header(self):
        return self.print_diskinfo_fmt % (
          "hbtl",
          "devname",
          "size",
          "dev",
          "vendor",
          "model",
        )


    def print_diskinfo_header(self):
        print(self.diskinfo_header())


    def scanscsi(self, hba=None, target=None, lun=None):
        raise ex.Error("not implemented")


