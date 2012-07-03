from subprocess import *
import os

repo_subdir = "rpms"

def update(fpath):
    if os.path.exists('/etc/debian_version'):
        return update_deb(fpath)
    elif os.path.exists('/etc/SuSE-release') or \
         os.path.exists('/etc/redhat-release'):
        return update_rpm(fpath)

def update_deb(fpath):
    cmd = ['dpkg', '-i', fpath]
    print ' '.join(cmd)
    p = Popen(cmd)
    p.communicate()
    return p.returncode

def update_rpm(fpath):
    cmd = ['rpm', '-Uvh', fpath, '--force', '--nodeps']
    print ' '.join(cmd)
    p = Popen(cmd)
    p.communicate()
    return p.returncode
