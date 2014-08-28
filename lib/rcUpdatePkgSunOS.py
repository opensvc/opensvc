from subprocess import *
from rcUtilitiesSunOS import get_os_ver
import os

repo_subdir = "sunos-pkg"

def update(fpath):
    # check downloaded package integrity
    cmd = ['pkgchk', '-d', fpath, 'all']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1

    cmd = ['pkgrm', '-n', 'opensvc']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1
    if get_os_ver() < 10:
        opts = ''
    else:
        opts = '-G'
    cmd = 'echo y | pkgadd %s -d %s all' % (opts, fpath)
    print(cmd)
    return os.system(cmd)
