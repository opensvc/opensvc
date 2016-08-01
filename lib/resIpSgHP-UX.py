Res = __import__("resIpHP-UX")

class Ip(Res.Ip):

    def start(self):
        return 0

    def stop(self):
        return 0

if __name__ == "__main__":
    for c in (Ip,) :
        help(c)

