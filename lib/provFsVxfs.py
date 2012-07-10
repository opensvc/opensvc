from provFs import ProvisioningFs

class ProvisioningFsVxfs(ProvisioningFs):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    info = ['fstyp']
