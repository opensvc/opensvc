import provFs

class Prov(provFs.Prov):
    mkfs = ['mkfs.ext2', '-F', '-q']
    info = ['tune2fs', '-l']
