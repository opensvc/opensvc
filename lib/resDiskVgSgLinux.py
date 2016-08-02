Res = __import__("resDiskVgLinux")

class Disk(Res.Disk):

    def start(self):
        return 0

    def stop(self):
        return 0

