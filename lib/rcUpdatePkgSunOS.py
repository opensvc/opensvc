from subprocess import *

repo_subdir = "pkg"

def update(fpath):
    cmd = ['pkgrm', '-n', 'opensvc']
    print ' '.join(cmd)
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1
    cmd = "echo y | pkgadd -G -d %s all"%fpath
    print cmd
    p = Popen(cmd, shell=True)
    p.communicate()
    return p.returncode
