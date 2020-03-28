from .mounts import BaseMounts, Mount


class Mounts(BaseMounts):
    def __init__(self, wmi=None):
        self.wmi = wmi
        super(Mounts, self).__init__()

    def match_mount(self, i, dev, mnt):
        """Given a line of 'mount' output, returns True if (dev, mnt) matches
        this line. Returns False otherwise.
        """
        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        return False

    def parse_mounts(self, wmi=None):
        if self.wmi is None:
            import foreign.wmi as wmi
            self.wmi = wmi.WMI()
        mounts = []
        for volume in self.wmi.Win32_Volume():
            dev = volume.DeviceID
            mnt = volume.Name
            if mnt is None:
                mnt = ""
            type = volume.FileSystem
            mnt_opt = "NULL"  # quoi mettre d autre...
            m = Mount(dev, mnt, type, mnt_opt)
            mounts.append(m)
        return mounts


if __name__ == "__main__":
    # help(Mounts)
    for m in Mounts():
        print(m)
