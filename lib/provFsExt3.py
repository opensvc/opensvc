from provFs import ProvisioningFs

class ProvisioningFsExt4(ProvisioningFs):
    mkfs = ['mkfs.ext4', '-F', '-q']
    info = ['tune2fs', '-l']
