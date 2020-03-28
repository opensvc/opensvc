from env import Env
from utilities.proc import justcall

"""
Subset               Status                 Description
------               ------                 -----------
IOSFRBASE540         installed              French Base System (French Support - Operating System)
IOSFRCDEHLP540       not installed          French CDE Online Help (French Support - Windowing Environment)
IOSFRCDEMIN540       installed              French CDE Minimum Runtime Environment(French Support - Windowing Environment)
IOSFRX11540          installed              French Basic X Environment (French Support - Windowing Environment)
"""

def _list():
    cmd = ['setld', '-i']
    out, err, ret = justcall(cmd)
    pkg = []
    patch = []
    pkgarch = ""
    pkgvers = ""
    if ret != 0:
        return []
    lines = out.split('\n')
    if len(lines) < 3:
        return []
    for line in lines[2:]:
        if "installed" not in line or "not installed" in line:
            continue
        name = line.split()[0]
        if "Patch:" in line:
            x = [Env.nodename, name, pkgvers]
            patch.append(x)
        else:
            x = [Env.nodename, name, pkgvers, pkgarch]
            pkg.append(x)
    return pkg, patch

def listpkg():
    return _list()[0]

def listpatch():
    return _list()[1]
