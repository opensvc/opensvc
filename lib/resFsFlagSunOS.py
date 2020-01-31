import os

from rcUtilities import lazy
import resFsFlagAbstract


class Fs(resFsFlagAbstract.Fs):
    @lazy
    def base_flag_d(self):
        flag_dir = os.path.join(os.sep, "system", "volatile")
        if os.path.exists(flag_dir):
            return os.path.join(flag_dir, "opensvc")
        else:
            return os.path.join(os.sep, "var", "run", "opensvc")
