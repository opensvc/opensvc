import datetime
import os
from collections import namedtuple
from stat import *

from env import Env
from utilities.proc import justcall, which


def listpkg_dummy():
    print("pushpkg not supported on this system")
    cmd = ['true']
    return []

def listpkg_snap():
    """
    Example:

        Name      Version    Rev   Tracking  Publisher   Notes
        core      16-2.35.4  5662  stable    canonical*  core
        inkscape  0.92.3     4274  stable    inkscape*   -
        skype     8.32.0.44  60    stable    skype*      classic

    """
    if not which("snap"):
        return []
    cmd = ["snap", "list", "--unicode=never", "--color=never"]
    out, err, ret = justcall(cmd)
    lines = []
    for line in out.splitlines():
        if line.startswith('Name'):
            header = namedtuple("header", line)
            continue
        _data = header._make(line.split())
        lines.append([
            Env.nodename,
            _data.Name,
            "%s rev %s" % (_data.Version, _data.Rev),
            "",
            "snap",
            "",
        ])
    return lines

def listpkg_rpm():
    if not which("rpm"):
        return []
    cmd = ['rpm', '-qai', '--queryformat=XX%{n} %{v}-%{r} %{arch} rpm %{installtime}\n']
    out, err, ret = justcall(cmd)
    lines = []
    for line in out.split('\n'):
        if line.startswith('Signature'):
            sig = line.split()[-1].strip()
            continue
        elif not line.startswith('XX'):
            continue
        line = line[2:]
        l = line.split()
        if len(l) < 5:
            continue
        try:
            l[4] = datetime.datetime.fromtimestamp(int(l[4])).strftime("%Y-%m-%d %H:%M:%S")
        except:
            l[4] = ""
        x = [Env.nodename] + l + [sig]
        lines.append(x)
    return lines

def listpkg_deb():
    if not which("dpkg"):
        return []
    cmd = ['dpkg', '-l']
    out, err,ret = justcall(cmd)
    lines = []
    arch = ""
    for line in out.splitlines():
        l = line.split()
        if len(l) < 4:
            continue
        if l[0] != "ii":
            continue
        x = [Env.nodename] + l[1:3] + [arch, "deb"]
        try:
            t = os.stat("/var/lib/dpkg/info/"+l[1]+".list")[ST_MTIME]
            t = datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
        except:
            t = ""
        x.append(t)
        lines.append(x)
    return lines

def listpkg():
    data = listpkg_deb()
    data += listpkg_rpm()
    data += listpkg_snap()
    return data

def listpatch():
    return []
