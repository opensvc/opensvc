#
# Copyright (c) 2014 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2014 Arnaud Veron <arnaud.veron@opensvc.com>
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

class Mounts(rcMounts.Mounts):

    def match_mount(self, i, dev, mnt):
        """Given a line of 'mount' output, returns True if (dev, mnt) matches
        this line. Returns False otherwise.
        """
        if i.mnt != mnt:
            return False
        if i.dev == dev:
            return True
        return False

    def __init__(self, wmi=None):
        if wmi is None:
            import wmi
            wmi = wmi.WMI()
        self.mounts = []
        for volume in wmi.Win32_Volume():
            dev = volume.DeviceID
            mnt = volume.Name
            if mnt is None:
                mnt = ""
            type = volume.FileSystem
            mnt_opt = "NULL"    # quoi mettre d autre...
            m = rcMounts.Mount(dev, mnt, type, mnt_opt)
            self.mounts.append(m)

if __name__ == "__main__" :
    #help(Mounts)
    for m in Mounts():
        print(m)
