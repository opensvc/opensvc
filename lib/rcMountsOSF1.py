import rcMounts
from rcUtilities import justcall

class Mounts(rcMounts.Mounts):

    def match_mount(self, i, dev, mnt):
        """Given a line of 'mount' output, returns True if (dev, mnt) matches
        this line. Returns False otherwize. Also care about weirdos like loops
        and binds, ...
        """
        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        return False

    def __init__(self):
        self.mounts = []
        out, err, ret = justcall(['mount'])
        for l in out.split('\n'):
            l = l.replace(', ', ',')
            if len(l.split()) != 6:
                return
            dev, null, mnt, null, type, mnt_opt = l.split()
            m = rcMounts.Mount(dev, mnt, type, mnt_opt.strip('()'))
            self.mounts.append(m)

if __name__ == "__main__" :
    help(Mounts)
    for m in Mounts():
        print(m)
