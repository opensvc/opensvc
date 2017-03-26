import rcExceptions as ex

class diskInfo(object):
    """Parent class for diskInfo OS"""

    print_diskinfo_fmt = "%-12s %-8s %12s MB %-8s %-8s %-16s"

    def disk_id(self, dev):
        return "tbd"

    def disk_vendor(self, dev):
        return "tbd"

    def disk_model(self, dev):
        return "tbd"

    def disk_size(self, dev):
        return "tbd"

    def print_diskinfo_header(self):
        print(self.print_diskinfo_fmt%(
          "hbtl",
          "devname",
          "size",
          "dev",
          "vendor",
          "model",
        ))

    def scanscsi(self, hba=None, target=None, lun=None):
        raise ex.excError("not implemented")


