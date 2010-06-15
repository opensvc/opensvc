#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#

import rcMounts
#import resLoopAIX as Res
from rcUtilities import *
from subprocess import Popen

def get_src_dir_dev(dev):
    """Given a directory path, return its hosting device
    """
    process = Popen(['df', dev], stdout=PIPE, stderr=STDOUT, close_fds=True)
    buff = process.communicate()
    out = buff[0]
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
#        if i.dev in Res.file_to_loop(dev):
#            return True
        if is_bind and i.dev == src_dir_dev:
            return True
        return False

    def __init__(self):
        self.mounts = []
        (ret, out) = call(['mount'])
        lines = out.split('\n')
        if len(lines) < 3:
            return
        for l in lines[2:]:
            if len(l) == 0:
                continue
            x = l.split()
            if x[0][0] == '/':
                dev, mnt, type, null, null, null, mnt_opt = l.split()
            else:
                node, dev, mnt, type, null, null, null, mnt_opt = l.split()
            m = rcMounts.Mount(dev, mnt, type, mnt_opt)
            self.mounts.append(m)

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
"""

if __name__ == "__main__" :
    help(Mounts)
    for m in Mounts():
        print m
