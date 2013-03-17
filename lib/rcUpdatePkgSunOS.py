from subprocess import *

repo_subdir = "pkg"

def update(fpath):
    cmd = ['pkgrm', '-n', 'opensvc']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1
    cmd1 = ['yes']
    cmd2 = ['pkgadd', '-G', '-d', fpath, 'all']
    print(' '.join(cmd1) + '|' ' '.join(cmd2))
    p1 = Popen(cmd1, stdout=PIPE)
    p2 = Popen(cmd2, stdin=p1.stdout)
    p2.communicate()
    p1.terminate()
    return p2.returncode

