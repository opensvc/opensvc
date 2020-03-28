import foreign.wmi as wmi

from .devtree import DevTree as BaseDevTree

class DevTree(BaseDevTree):

    def load_diskdrive(self):
        if not hasattr(self, 'wmi'):
            self.wmi = wmi.WMI()
        for drive in self.wmi.WIN32_DiskDrive():
            d = self.add_dev(drive.DeviceId, int(drive.size)//1024, "linear")
            d.set_devpath(drive.DeviceId)

    def load(self, di=None):
        self.load_diskdrive()

    def blacklist(self, devname):
        return False

if __name__ == "__main__":
    tree = DevTree()
    tree.load()
    #print(tree)
    tree.print_tree_bottom_up()
    #print(map(lambda x: x.alias, tree.get_top_devs()))
