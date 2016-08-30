import provFs

class ProvisioningFs(provFs.ProvisioningFs):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    info = ['fstyp']
