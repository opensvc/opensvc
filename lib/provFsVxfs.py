import provFs

class Prov(provFs.Prov):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    info = ['fstyp']
