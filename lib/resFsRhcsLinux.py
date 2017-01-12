from rcGlobalEnv import rcEnv
Res = __import__("resFsLinux")

class Mount(Res.Mount):
    def start(self):
        pass

    def stop(self):
        pass

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

