#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@opensvc.com>'
# Copyright (c) 2014 Arnaud Veron <arnaud.veron@opensvc.com>'
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
import plistlib

from rcGlobalEnv import *
from rcUtilities import call, which
import rcStatus
import resLoop as Res
import rcExceptions as ex

def file_to_loop(f):
    """Given a file path, returns the disk device associated. For example,
    /path/to/file => /dev/disk0s1
    """
    if which('hdiutil') is None:
        return []
    if not os.path.isfile(f):
        return []
    (ret, out, err) = call(['hdiutil', 'info', '-plist'])
    if ret != 0:
        return []

    devs= []
    pl = plistlib.readPlistFromString(out)
    for image in pl['images']:
        if image.get('image-path') == f:
            for se in image['system-entities']:
                if se.get('mount-point') is not None:
                    diskdevice = se.get('dev-entry')
                    if diskdevice is not None:
                        devs.append(diskdevice)
                    else:
                        return []
    
    return devs
