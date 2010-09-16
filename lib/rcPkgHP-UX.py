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
from rcUtilities import call, which
from rcGlobalEnv import rcEnv

def listpkg():
    if which('swlist') is None:
        return []
    cmd = ['swlist', '-l', 'product']
    (ret, out) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 2:
            continue
        if line[0] == '#':
            continue
        x = [rcEnv.nodename, l[0], l[1], '']
        lines.append(x)
    return lines

def listpatch():
    return []
