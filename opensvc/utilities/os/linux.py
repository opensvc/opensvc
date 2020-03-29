import ctypes

SYS_gettid = 186

class LibC:
    _libc = None

    def _load_lib(self):
        if self._libc:
            return
        self._libc = ctypes.cdll.LoadLibrary("libc.so.6")

    def syscall(self, id):
        self._load_lib()
        return self._libc.syscall(id)

libc = LibC()

def get_tid():
    return libc.syscall(SYS_gettid)
