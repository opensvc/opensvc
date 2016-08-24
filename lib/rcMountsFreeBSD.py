import rcMounts
from rcUtilities import *
from subprocess import Popen

def get_src_dir_dev(dev):
    """Given a directory path, return its hosting device
    """
    process = Popen(['df', '-l', dev], stdout=PIPE, stderr=STDOUT, close_fds=True)
    buff = process.communicate()
    out = bdecode(buff[0])
    if "/" not in out:
        return
    i = out.index('/')
    return out[i:].split()[0]

class Mounts(rcMounts.Mounts):

    def match_mount(self, i, dev, mnt):
        """Given a line of 'mount' output, returns True if (dev, mnt) matches
        this line. Returns False otherwize. Also care about weirdos like loops
        and binds, ...
        """
        if os.path.isdir(dev):
            is_bind = True
            src_dir_dev = get_src_dir_dev(dev)
        else:
            is_bind = False

        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        if is_bind and i.dev == src_dir_dev:
            return True
        return False

    def __init__(self):
        self.mounts = []
        (ret, out, err) = call(['mount'])
        for l in out.split('\n'):
            words = l.split()
            if len(words) < 4:
                return
            dev = words[0]
            mnt = words[2]
            opts = ' '.join(words[3:]).strip('(').strip(')').split(', ')
            type = opts[0]
            if len(opts) < 3:
                mnt_opt = ''
            else:
                mnt_opt = ','.join(opts[2:])
            m = rcMounts.Mount(dev, mnt, type, mnt_opt)
            self.mounts.append(m)

if __name__ == "__main__" :
    help(Mounts)
    for m in Mounts():
        print(m)
