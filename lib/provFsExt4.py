import provFs

class Prov(provFs.Prov):
    mkfs = ['mkfs.ext4', '-F', '-q']
    info = ['tune2fs', '-l']
