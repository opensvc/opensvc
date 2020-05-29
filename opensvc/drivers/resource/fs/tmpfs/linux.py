from ..linux import Fs

DRIVER_GROUP = "fs"
DRIVER_BASENAME = "tmpfs"

class FsTmpfs(Fs):
    def check_stat(self):
        return True

