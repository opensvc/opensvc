from .. import adder as base_adder
from ..linux import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsExt2)

class FsExt2(Fs):
    mkfs = ['mkfs.ext2', '-F', '-q']
    info = ['tune2fs', '-l']

