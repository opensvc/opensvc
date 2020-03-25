from subprocess import *

repo_subdir = "rpms"

def update(fpath):
    cmd = ['rpm', '-U', fpath, '--force', '--ignoreos', '--nodeps']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode
