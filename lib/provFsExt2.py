from provFs import ProvisioningFs

class ProvisioningFsExt2(ProvisioningFs):
    mkfs = ['mkfs.ext2', '-F', '-q']
    info = ['tune2fs', '-l']
