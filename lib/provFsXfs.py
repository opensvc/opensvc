import provFs

class Prov(provFs.Prov):
    info = ['xfs_admin', '-l']
    mkfs = ['mkfs.xfs', '-f', '-q']

