from .. import adder as base_adder
from ..linux import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsExt3)

class FsExt3(Fs):
    mkfs = ['mkfs.ext3', '-F', '-q']
    info = ['tune2fs', '-l']

