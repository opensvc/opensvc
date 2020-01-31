import os

from rcUtilities import lazy
import resFsFlagAbstract


class Fs(resFsFlagAbstract.Fs):
    @lazy
    def base_flag_d(self):
        return os.path.join(os.sep, "dev", "shm", "opensvc")
