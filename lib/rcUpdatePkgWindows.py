from subprocess import *

repo_subdir = "msi"

def update(fpath):
    cmd = ['msiexec', '/quiet', '/i', fpath, 'REINSTALL=ALL', 'REINSTALLMODE=vomus']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode
