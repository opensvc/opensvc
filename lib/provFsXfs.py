import provFs

class ProvisioningFs(provFs.ProvisioningFs):
    info = ['xfs_admin', '-l']
    mkfs = ['mkfs.xfs', '-f', '-q']

