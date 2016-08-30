import rcMounts
from rcLoopLinux import file_to_loop
from rcUtilities import *

class Mounts(rcMounts.Mounts):
    df_one_cmd = ['df', '-l']

    def match_mount(self, i, dev, mnt):
        """Given a line of 'mount' output, returns True if (dev, mnt) matches
        this line. Returns False otherwize. Also care about weirdos like loops
        and binds, ...
        """
        if os.path.isdir(dev):
            is_bind = True
            src_dir_dev = self.get_src_dir_dev(dev)
        else:
            is_bind = False

        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        if i.dev in file_to_loop(dev):
            return True
        if is_bind and i.dev == src_dir_dev:
            return True
        return False

    def __init__(self):
        self.mounts = []
        (ret, out, err) = call(['mount'])
        out = out.replace(" (deleted)", "")
        for l in out.split('\n'):
            if len(l.split()) != 6:
                return
            dev, null, mnt, null, type, mnt_opt = l.split()
            m = rcMounts.Mount(dev, mnt, type, mnt_opt.strip('()'))
            self.mounts.append(m)

if __name__ == "__main__" :
    help(Mounts)
    for m in Mounts():
        print(m)
