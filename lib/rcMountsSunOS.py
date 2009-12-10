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
        (ret, out) = call(['mount','-p'])
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
    print M
    
