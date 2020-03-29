from .. import adder as base_adder
from ..sunos import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsVxfs)

class FsVxfs(Fs):
    mkfs = ['newfs', '-F', 'vxfs', '-o', 'largefiles', '-b', '8192']
    info = ['fstyp']

