from subprocess import *

repo_subdir = "msi"

def update(fpath):
    cmd = ['msiexec', '/i', fpath, 'REINSTALL=ALL', 'REINSTALLMODE=vomus']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode
