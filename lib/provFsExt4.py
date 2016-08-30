import provFs

class ProvisioningFs(provFs.ProvisioningFs):
    mkfs = ['mkfs.ext4', '-F', '-q']
    info = ['tune2fs', '-l']
