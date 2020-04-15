import os
from . import BaseCapabilities
from env import Env

REFRESH_FLAGS = [
   "/var/log/dpkg.log",
   "/var/lib/rpm/Basenames",
]

class Capabilities(BaseCapabilities):
    def need_refresh(self):
        try:
            last = os.path.getmtime(Env.paths.capabilities)
        except OSError:
            return True
        for flag in REFRESH_FLAGS:
            try:
                mtime = os.path.getmtime(flag)
            except OSError:
                continue
            if last < mtime:
                return True
        return False

