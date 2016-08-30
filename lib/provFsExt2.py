import provFs

class ProvisioningFs(provFs.ProvisioningFs):
    mkfs = ['mkfs.ext2', '-F', '-q']
    info = ['tune2fs', '-l']
