from . import FsZfsMixin
from .. import adder as base_adder
from ..sunos import Fs

def adder(svc, s):
    base_adder(svc, s, drv=FsZfs)

class FsZfs(FsZfsMixin, Fs):
    pass

