import os
import datetime
from rcUtilities import call, which
from rcGlobalEnv import rcEnv
from stat import *

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

def listpkg_ips():
    if which('uname') is None:
        return []
    cmd = ['uname', '-p']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    arc = out.split('\n')[0]
    if which('pkg') is None:
        return []
    cmd = ['pkg', 'list', '-H']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) != 3:
            continue
        x = [rcEnv.nodename, l[0], l[1], arc, "ips", ""]
        lines.append(x)
    return lines

def listpkg_legacy():
    if which('pkginfo') is None:
        return []
    cmd = ['pkginfo', '-l']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) != 2:
            continue
        l = map(lambda x: x.strip(), l)
        f = l[0]
        if f == "PKGINST":
            x = [rcEnv.nodename, l[1], "", "", "pkg", ""]
        elif f == "VERSION":
            x[2] = l[1]
        elif f == "ARCH":
            x[3] = l[1]
            lines.append(x)

    for i, x in enumerate(lines):
        # pkg install date
        try:
            t = os.stat("/var/sadm/pkg/"+x[1])[ST_MTIME]
            t = datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            t = ""
        lines[i][5] = t

    return lines

def listpkg():
    return listpkg_legacy() + listpkg_ips()

def listpatch():
    """
    Patch: patchnum-rev Obsoletes: num-rev[,patch-rev]... Requires: .... Incompatibles: ... Packages: ...
    """
    if which('showrev') is None:
        return []
    cmd = ['showrev', '-p']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    nodename = rcEnv.nodename
    for line in out.split('\n'):
        l = line.split(' ')
        if len(l) > 3:
            p = l[1].split('-')
            if len(p) != 2:
                continue
            else:
                lines.append( [ nodename , p[0], p[1] ] )

    for i, x in enumerate(lines):
        # pkg install date
        try:
            t = os.stat("/var/sadm/patch/"+x[1])[ST_MTIME]
            t = datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
        except:
            t = ""
        lines[i].append(t)

    return lines

if __name__ == "__main__" :
    print(listpatch())
