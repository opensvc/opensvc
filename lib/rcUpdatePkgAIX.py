from subprocess import *

repo_subdir = "rpms"

def update(fpath):
    cmd = ['rpm', '-Uvh', fpath, '--force']
    print ' '.join(cmd)
    p = Popen(cmd)
    p.communicate()
    return p.returncode
