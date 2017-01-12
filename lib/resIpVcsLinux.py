Res = __import__("resIpLinux")

class Ip(Res.Ip):
    def start(self):
        return 0

    def stop(self):
        return 0

