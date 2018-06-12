import provFs
from rcUtilities import which

class Prov(provFs.Prov):
    if which("newfs"):
        mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    elif which("mkfs.vxfs"):
        mkfs = ['mkfs.vxfs', '-o', 'largefiles,bsize=8192']
    info = ['fstyp']
