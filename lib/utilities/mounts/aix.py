import os

from utilities.proc import justcall
from .mounts import BaseMounts, Mount


class Mounts(BaseMounts):
    df_one_cmd = ['df']

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
            src_dir_dev = None

        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        #        if i.dev in Res.file_to_loop(dev):
        #            return True
        if is_bind and i.dev == src_dir_dev:
            return True
        return False

    def parse_mounts(self):
        mounts = []
        out, err, ret = justcall(['mount'])
        lines = out.split('\n')
        if len(lines) < 3:
            return
        for l in lines[2:]:
            if len(l) == 0:
                continue
            x = l.split()
            if x[0] == '-hosts':
                continue
            elif x[0][0] == '/':
                dev, mnt, type, null, null, null, mnt_opt = l.split()
            else:
                v = l.split()
                if len(v) == 7:
                    node, dev, mnt, type, null, null, null = l.split()
                    mntopt = ""
                if len(v) == 8:
                    node, dev, mnt, type, null, null, null, mnt_opt = l.split()
                else:
                    continue
            m = Mount(dev, mnt, type, mnt_opt)
            mounts.append(m)
        return mounts


"""
  node       mounted        mounted over    vfs       date        options
-------- ---------------  ---------------  ------ ------------ ---------------
         /dev/hd4         /                jfs2   Jun 14 19:42 rw,log=/dev/hd8
         /dev/hd2         /usr             jfs2   Jun 14 19:42 rw,log=/dev/hd8
         /dev/hd9var      /var             jfs2   Jun 14 19:42 rw,log=/dev/hd8
         /dev/hd3         /tmp             jfs2   Jun 14 19:42 rw,log=/dev/hd8
         /dev/hd1         /home            jfs2   Jun 14 19:48 rw,log=/dev/hd8
         /proc            /proc            procfs Jun 14 19:48 rw
         /dev/hd10opt     /opt             jfs2   Jun 14 19:48 rw,log=/dev/hd8

  node       mounted        mounted over    vfs       date        options
-------- ---------------  ---------------  ------ ------------ ---------------
         /dev/hd4         /                jfs2   Nov 29 11:22 rw,log=/dev/hd8
         /dev/hd2         /usr             jfs2   Nov 29 11:22 rw,log=/dev/hd8
         /dev/hd9var      /var             jfs2   Nov 29 11:22 rw,log=/dev/hd8
         /dev/hd3         /tmp             jfs2   Nov 29 11:23 rw,log=/dev/hd8
         /dev/hd1         /home            jfs2   Nov 29 11:23 rw,log=/dev/hd8
         /proc            /proc            procfs Nov 29 11:23 rw
         /dev/hd10opt     /opt             jfs2   Nov 29 11:23 rw,log=/dev/hd8
         /dev/lv_logs     /logs            jfs2   Nov 29 11:23 rw,log=/dev/hd8
         /dev/lv_moteurs  /moteurs         jfs2   Nov 29 11:23 rw,log=/dev/hd8
         -hosts           /net             autofs Nov 29 11:23 nosuid,vers=3,rw,nobrowse,ignore
x64lmwbief4 /install/outils  /mnt             nfs3   Dec 07 11:53

"""

if __name__ == "__main__":
    help(Mounts)
    for m in Mounts():
        print(m)
