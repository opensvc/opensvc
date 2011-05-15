#!/usr/bin/python2.6
#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
from rcUtilities import call, which
from rcGlobalEnv import rcEnv

"""
format:

package-id: com.apple.pkg.X11User
version: 10.6.0.1.1.1238328574
volume: /
location: /
install-time: 1285389505
groups: com.apple.snowleopard-repair-permissions.pkg-group com.apple.FindSystemFiles.pkg-group 
"""

def pkgversion(package):
    cmd = ['pkgutil', '--pkg-info', package]
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    for line in out.split('\n'):
        l = line.split(': ')
        if len(l) != 2:
            continue
        if l[0] == 'version':
            return l[1]
    return ''

def listpkg():
    if which('pkgutil') is None:
        return []
    cmd = ['pkgutil', '--packages']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        if len(line) == 0:
            continue
        x = [rcEnv.nodename, line, pkgversion(line), ""]
        lines.append(x)
    return lines

def listpatch():
    return []

