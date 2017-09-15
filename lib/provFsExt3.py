import provFs

class Prov(provFs.Prov):
    mkfs = ['mkfs.ext3', '-F', '-q']
    info = ['tune2fs', '-l']
