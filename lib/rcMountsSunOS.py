import rcMounts
from rcUtilities import *


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
        (ret, out, err) = call(['mount','-p'], outdebug=False)
        for line in out.split('\n'):
            words=line.split()
            if len(words) < 6 :
                continue
            elif words[1]+words[4] != '--' :
                # ignore mount line with space in mountpoint or dev
                continue
            elif len(words) == 6 :
                words.append('-')
            dev, null, mnt, type, null, null, mnt_opt = words
            m = rcMounts.Mount(dev, mnt, type, mnt_opt.strip('()'))
            self.mounts.append(m)

if __name__ == "__main__" :
    help(Mounts)
    M=Mounts()
    print(M)

