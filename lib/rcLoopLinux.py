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
import re

from rcGlobalEnv import *
from rcUtilities import call, which
import rcStatus
import resLoop as Res
import rcExceptions as ex

def file_to_loop(f):
    """Given a file path, returns the loop device associated. For example,
    /path/to/file => /dev/loop0
    """
    if which('losetup') is None:
        return []
    if not os.path.isfile(f):
        return []
    if rcEnv.sysname != 'Linux':
        return []
    (ret, out, err) = call(['losetup', '-j', f])
    if len(out) == 0:
        return []
    """ It's possible multiple loopdev are associated with the same file
    """
    devs= []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) == 0:
            continue
        if len(l[0]) == 0:
            continue
        if not os.path.exists(l[0]):
            continue
        devs.append(l[0])
    return devs

