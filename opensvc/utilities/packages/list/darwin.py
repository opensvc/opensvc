from env import Env
from utilities.proc import call, which


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
        x = [Env.nodename, line, pkgversion(line), ""]
        lines.append(x)
    return lines

def listpatch():
    return []
