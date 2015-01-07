from __future__ import print_function
import os
import tarfile

repo_subdir = "tar"

def update(fpath):
    oldpath = os.getcwd()
    os.chdir(os.sep)
    tar = tarfile.open(fpath)
    try:
        tar.extractall()
        tar.close()
    except:
        try:
            os.unlink(fpath)
        except:
            pass
        print("failed to unpack", file=sys.stderr)
        return 1
    try:
        os.unlink(fpath)
    except:
        pass

    cmd = '/opt/opensvc/bin/python /opt/opensvc/bin/postinstall'
    return os.system(cmd)
