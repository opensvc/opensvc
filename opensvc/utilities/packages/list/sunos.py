import datetime
import os
from stat import ST_MTIME

from env import Env
from utilities.proc import justcall, which


def listpkg_ips():
    """
    Return a list of ips packages installed.
    """

    #
    #   PKGINST:  SUNWzoneu
    #      NAME:  Solaris Zones (Usr)
    #  CATEGORY:  system
    #      ARCH:  i386
    #   VERSION:  11.11,REV=2009.04.08.17.26
    #    VENDOR:  Sun Microsystems, Inc.
    #      DESC:  Solaris Zones Configuration and Administration
    #   HOTLINE:  Please contact your local service provider
    #    STATUS:  completely installed
    #

    if which('uname') is None:
        return []
    cmd = ['uname', '-p']
    out, _, _ = justcall(cmd)
    arc = out.split('\n')[0]
    if which('pkg') is None:
        return []
    cmd = ['pkg', 'list', '-H']
    out, _, _ = justcall(cmd)
    lines = []
    for line in out.split('\n'):
        elems = line.split()
        if len(elems) != 3:
            continue
        data = [Env.nodename, elems[0], elems[1], arc, "ips", ""]
        lines.append(data)
    return lines

def listpkg_legacy():
    """
    Return a list of legacy packages installed.
    """
    if which('pkginfo') is None:
        return []
    cmd = ['pkginfo', '-l']
    out, _, _ = justcall(cmd)
    lines = []
    for line in out.splitlines():
        elems = line.split(':', 1)
        if len(elems) != 2:
            continue
        key, val = [elem.strip() for elem in elems]
        if key == "PKGINST":
            data = [Env.nodename, val, "", "", "pkg", ""]
        elif key == "VERSION":
            data[2] = val
        elif key == "ARCH":
            data[3] = val
            lines.append(data)

    for idx, line in enumerate(lines):
        # pkg install date
        try:
            mtime = os.stat("/var/sadm/pkg/"+line[1])[ST_MTIME]
            mtime = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            mtime = ""
        lines[idx][5] = mtime

    return lines

def listpkg():
    """
    Return a list of ips and legacy packages installed.
    """
    return listpkg_legacy() + listpkg_ips()

def listpatch():
    """
    Return a list of patches installed.

    Patch: patchnum-rev Obsoletes: num-rev[,patch-rev]... Requires: .... Incompatibles: ... Packages: ...
    """
    if which('showrev') is None:
        return []
    cmd = ['showrev', '-p']
    out, _, _ = justcall(cmd)
    lines = []
    nodename = Env.nodename
    for line in out.splitlines():
        elems = line.split(' ')
        if len(elems) > 3:
            _elems = elems[1].split('-')
            if len(_elems) != 2:
                continue
            else:
                lines.append([nodename , _elems[0], _elems[1]])

    for idx, line in enumerate(lines):
        # pkg install date
        try:
            mtime = os.stat("/var/sadm/patch/"+line[1])[ST_MTIME]
            mtime = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            mtime = ""
        lines[idx].append(mtime)

    return lines

if __name__ == "__main__" :
    print(listpkg())
