from subprocess import *

repo_subdir = "depot"

def update(fpath):
    cmd = ['swinstall', '-x', 'mount_all_filesystems=false', '-x', 'allow_downdate=true', '-s', fpath, '*']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode
