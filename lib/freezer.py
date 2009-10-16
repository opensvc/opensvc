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
import os
import glob

def dummy():
    return

class Freezer:
    flag_dir = os.path.join(os.path.dirname(__file__), '..', 'var')
    base_flag = os.path.join(flag_dir, 'FROZEN')
    flag = base_flag

    def frozen(self):
        if os.path.exists(self.flag) or os.path.exists(self.base_flag):
            return True
        return False

    def freeze(self):
        open(self.flag, 'w').close() 

    def thaw(self):
        if self.flag != self.base_flag and os.path.exists(self.flag):
            os.unlink(self.flag)
            return
        for name in glob.glob(self.flag_dir + '/FROZEN*'):
            os.unlink(name)

    def __init__(self, name=''):
        if len(name) == 0:
            pass
        elif not os.path.exists(os.path.join(os.path.dirname(__file__), '..', 'etc', name)):
            self.freeze = dummy
            self.thaw = dummy
            self.frozen = dummy
        else:
            self.flag = self.flag + "." + name
