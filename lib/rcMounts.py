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

class Mount:
    def __init__(self, dev, mnt, type, mnt_opt):
        self.dev = dev
        self.mnt = mnt
        self.type = type
        self.mnt_opt = mnt_opt

    def __str__(self):
        return "Mount: dev[%s] mnt[%s] type[%s] options[%s]" % \
            (self.dev,self.mnt,self.type,self.mnt_opt)

class Mounts:
    mounts = []

    def __init__(self):
        """ OS dependent """
        pass

    def match_mount(self):
        """ OS dependent """
        pass

    def mount(self, dev, mnt):
        for i in self.mounts:
            if self.match_mount(i, dev, mnt):
                return i
        return None

    def has_mount(self, dev, mnt):
        for i in self.mounts:
            if self.match_mount(i, dev, mnt):
                return 0
        return 1

    def has_param(self, param, value):
        for i in self.mounts:
            if getattr(i, param) == value:
                return i
        return None

    def __str__(self):
        output="%s" % (self.__class__.__name__)
        for m in self.mounts:
            output+="\n  %s" % m.__str__()
        return output
