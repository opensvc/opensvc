from utilities.proc import justcall
from .mounts import BaseMounts, Mount


class Mounts(BaseMounts):
    df_one_cmd = ["df", "-l"]

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

    def parse_mounts(self):
        mounts = []
        out, err, ret = justcall(['mount', '-p'])
        for line in out.split('\n'):
            words = line.split()
            if len(words) < 6:
                continue
            elif words[1] + words[4] != '--':
                # ignore mount line with space in mountpoint or dev
                continue
            elif len(words) == 6:
                words.append('-')
            dev, null, mnt, type, null, null, mnt_opt = words
            m = Mount(dev, mnt, type, mnt_opt.strip('()'))
            mounts.append(m)
        return mounts


if __name__ == "__main__":
    help(Mounts)
    M = Mounts()
    print(M)
