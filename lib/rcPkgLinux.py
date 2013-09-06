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
import datetime
from stat import *

def listpkg_dummy():
    print("pushpkg supported on this system")
    return []

def listpkg_rpm():
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 5:
            continue
        try:
            l[4] = datetime.datetime.fromtimestamp(int(l[4])).strftime("%Y-%m-%d %H:%M:%S")
        except:
            l[4] = ""
        if len(l) == 6:
            try:
                l[5] = l[5][18:34]
            except:
                l[5] = ""
        x = [rcEnv.nodename] + l
        lines.append(x)
    return lines

def listpkg_deb():
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    arch = ""
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 4:
            continue
        if l[0] != "ii":
            continue
        x = [rcEnv.nodename] + l[1:3] + [arch, "deb"]
        try:
            t = os.stat("/var/lib/dpkg/info/"+l[1]+".list")[ST_MTIME]
            t = datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
        except:
            t = ""
        x.append(t)
        lines.append(x)
    return lines

if which('dpkg') is not None:
    cmd = ['dpkg', '-l']
    listpkg = listpkg_deb
elif which('rpm') is not None:
    cmd = ['rpm', '-qa', '--queryformat=%{n} %{v}-%{r} %{arch} rpm %{installtime} %{SIGGPG}\n']
    listpkg = listpkg_rpm
else:
    cmd = ['true']
    listpkg = listpkg_dummy

def listpatch():
    return [] 
