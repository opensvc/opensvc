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
        flag_dir = os.path.join(os.sep, "system", "volatile")
        if os.path.exists(flag_dir):
            return os.path.join(flag_dir, "opensvc")
        else:
            return os.path.join(os.sep, "var", "run", "opensvc")