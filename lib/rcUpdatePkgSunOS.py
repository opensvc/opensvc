from subprocess import *
from rcUtilitiesSunOS import get_os_ver

repo_subdir = "pkg"

def update(fpath):
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
    cmd = ['pkgadd', opts, '-d', fpath, 'all']
    print(' '.join(cmd))
    p = Popen(cmd, stdout=PIPE, stdin=PIPE)
    while p.returncode is None:
        p.stdin.write("y\n")
        p.poll()
    return p.returncode
