from .diskinfo import BaseDiskInfo

class DiskInfo(BaseDiskInfo):
    disk_ids = {}

    def __init__(self, deferred=False):
        pass

    def disk_id(self, dev):
        print("%s:disk_id TODO"%__file__)
        return ""

    def disk_vendor(self, dev):
        print("%s:disk_vendor TODO"%__file__)
        return ""

    def disk_model(self, dev):
        print("%s:disk_model TODO"%__file__)
        return ""

    def disk_size(self, dev):
        print("%s:disk_size TODO"%__file__)
        return 0

