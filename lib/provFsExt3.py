from provFs import ProvisioningFs

class ProvisioningFsExt3(ProvisioningFs):
    mkfs = ['mkfs.ext3', '-F', '-q']
    info = ['tune2fs', '-l']
