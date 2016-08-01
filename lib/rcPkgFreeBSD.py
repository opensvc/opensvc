from rcUtilities import call, which
from rcGlobalEnv import rcEnv

def listpkg():
    if which('pkg_info') is None:
        return []
    cmd = ['pkg_info']
    (ret, out, err) = call(cmd, errlog=False, cache=True)
    lines = []
    for line in out.split('\n'):
        l = line.split()
        if len(l) < 2:
            continue
        nv = l[0].split('-')
        version = nv[-1]
        pkgname = '-'.join(nv[0:-1])
        x = [rcEnv.nodename, pkgname, version, '']
        lines.append(x)
    return lines

def listpatch():
    return [] 
