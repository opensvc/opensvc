from .. import adder as base_adder
from ..linux import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsExt4)

class FsExt4(Fs):
    mkfs = ['mkfs.ext4', '-F', '-q']
    info = ['tune2fs', '-l']

