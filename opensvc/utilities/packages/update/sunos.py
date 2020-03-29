import os
from subprocess import *

import utilities.os.sunos

repo_subdir = "sunos-pkg"

def gen_adminfile():
    filename = "/var/tmp/opensvc.adminfile"
    f = open (filename, 'w')
    f.write("mail=\n")
    f.write("instance=overwrite\n")
    f.write("partial=nocheck\n")
    f.write("runlevel=nocheck\n")
    f.write("idepend=nocheck\n")
    f.write("rdepend=nocheck\n")
    f.write("space=nocheck\n")
    f.write("setuid=nocheck\n")
    f.write("conflict=nocheck\n")
    f.write("action=nocheck\n")
    f.write("basedir=default\n")
    f.close()
    return filename

def update(fpath):
    try:
        # causing: pkgadd "unable to create temporary directory" errors
        del os.environ["TMPDIR"]
    except:
        pass
    # check downloaded package integrity
    cmd = ['pkgchk', '-d', fpath, 'all']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1

    # check if initially installed with pkgadd -G
    file = '/var/sadm/install/gz-only-packages'
    GlobalOnly = False
    if os.path.isfile(file):
        f = open(file)
        for line in f:
            if line.startswith("opensvc"):
                print("OpenSVC package was previously installed with pkgadd -G\n")
                GlobalOnly = True

    admin = gen_adminfile()
    cmd = ['pkgrm', '-a', admin, '-n', 'opensvc']
    print(' '.join(cmd))
    p = Popen(cmd)
    p.communicate()
    if p.returncode != 0:
        return 1
    osver = utilities.os.sunos.get_solaris_version()
    if osver < 10.0:
        opts = ''
    else:
        if GlobalOnly is True:
            opts = '-G'
        else:
            opts = ''
    opts += " -a %s " % admin
    cmd = 'pkgadd %s -d %s all' % (opts, fpath)
    print(cmd)
    return os.system(cmd)
