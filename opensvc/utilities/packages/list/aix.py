from env import Env
from utilities.proc import justcall


def listpkg():
    cmd = ['lslpp', '-Lc']
    out, err, ret = justcall(cmd)
    if ret != 0:
        return []
    lines = []
    for line in out.split('\n'):
        l = line.split(':')
        if len(l) < 5:
            continue
        pkgvers = l[2]
        pkgname = l[1].replace('-'+pkgvers, '')
        x = [Env.nodename, pkgname, pkgvers, '']
        lines.append(x)
    return lines

def listpatch():
    return []
