from utilities.proc import justcall
from .mounts import BaseMounts, Mount


class Mounts(BaseMounts):
    df_one_cmd = ['df', '-l']

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
        out, err, ret = justcall(['mount'])
        for l in out.split('\n'):
            l = l.replace(', ', ',')
            if len(l.split()) != 6:
                break
            dev, null, mnt, null, type, mnt_opt = l.split()
            m = Mount(dev, mnt, type, mnt_opt.strip('()'))
            mounts.append(m)
        return mounts


if __name__ == "__main__":
    help(Mounts)
    for m in Mounts():
        print(m)
