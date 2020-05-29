from subprocess import *

repo_subdir = "exe"

def update(fpath):
    cmd = [fpath, '/S']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode
