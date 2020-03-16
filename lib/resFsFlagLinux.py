import os

import resFsFlagAbstract

from rcUtilities import lazy
from svcBuilder import init_kwargs


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    r = Fs(**kwargs)
    svc += r


class Fs(resFsFlagAbstract.Fs):
    @lazy
    def base_flag_d(self):
        return os.path.join(os.sep, "dev", "shm", "opensvc")
