import provFs

class ProvisioningFs(provFs.ProvisioningFs):
    mkfs = ['mkfs.ext3', '-F', '-q']
    info = ['tune2fs', '-l']
