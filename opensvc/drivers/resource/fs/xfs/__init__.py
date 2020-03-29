from .. import adder as base_adder
from ..linux import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsXfs)

class FsXfs(Fs):
    info = ['xfs_admin', '-l']
    mkfs = ['mkfs.xfs', '-f', '-q']

