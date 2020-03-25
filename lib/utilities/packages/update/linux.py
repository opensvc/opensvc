import os
from subprocess import *


def update_deb(fpath):
    cmd = ['dpkg', '-i', fpath]
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode

def update_rpm(fpath):
    cmd = ['rpm', '-U', fpath, '--force', '--nodeps']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    return p.returncode

if os.path.exists('/etc/debian_version'):
    update = update_deb
    repo_subdir = "deb"
elif os.path.exists('/etc/SuSE-release') or \
     os.path.exists('/etc/SUSE-brand') or \
     os.path.exists('/etc/redhat-release'):
    repo_subdir = "rpms"
    update = update_rpm
