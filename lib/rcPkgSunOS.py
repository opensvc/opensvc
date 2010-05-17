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
import datetime
from rcUtilities import call, which
from rcGlobalEnv import rcEnv

"""
format:

   PKGINST:  SUNWzoneu
      NAME:  Solaris Zones (Usr)
  CATEGORY:  system
      ARCH:  i386
   VERSION:  11.11,REV=2009.04.08.17.26
    VENDOR:  Sun Microsystems, Inc.
      DESC:  Solaris Zones Configuration and Administration
   HOTLINE:  Please contact your local service provider
    STATUS:  completely installed

"""

def listpkg():
    if which('pkginfo') is None:
        return []
    cmd = ['pkginfo', '-l']
    (ret, out) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) != 2:
            continue
        f = l[0].strip()
        if f == "PKGINST":
            x = [rcEnv.nodename, l[1], "", ""]
        elif f == "VERSION":
            x[2] = l[1]
        elif f == "ARCH":
            x[3] = l[1]
        lines.append(x)
    return lines



