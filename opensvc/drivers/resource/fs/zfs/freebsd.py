from . import FsZfsMixin
from .. import adder as base_adder
from ..freebsd import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsZfs)

class FsZfs(FsZfsMixin, Fs):
    pass

