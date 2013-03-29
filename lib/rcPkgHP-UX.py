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
import datetime

def listpkg():
    if which('swlist') is None:
        return []
    lines = []
    for t in ('product', 'bundle'):
        lines += listpkg_t(t)
    return lines

def listpkg_t(t):
    cmd = ['swlist', '-l', t, '-a', 'revision', '-a', 'mod_time']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 3:
            continue
        if line[0] == '#':
            continue
        try:
            l[2] = datetime.datetime.fromtimestamp(int(l[2])).strftime("%Y-%m-%d %H:%M:%S")
        except:
            l[2] = ""
        x = [rcEnv.nodename, l[0], l[1], '', t, l[2]]
        lines.append(x)
    return lines

def listpatch():
    return []
