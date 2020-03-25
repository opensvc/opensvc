import os
from subprocess import *

repo_subdir = "macos-pkg"

def update(fpath):
    # macos installer expect a .pkg file extension
    pkgfile = fpath+'.pkg'
    print("renaming %s to %s"%(fpath, pkgfile))
    os.rename(fpath, pkgfile)
    cmd = ['installer', '-package', pkgfile, '-target', '/']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    try:
        os.unlink(pkgfile)
    except:
        pass
    return p.returncode
