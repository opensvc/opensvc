from .. import adder as base_adder
from ..linux import Fs
from utilities.proc import which

def adder(svc, s):
    base_adder(svc, s, drv=FsVxfs)

class FsVxfs(Fs):
    mkfs = ['mkfs.vxfs', '-o', 'largefiles,bsize=8192']
    info = ['fstyp']

