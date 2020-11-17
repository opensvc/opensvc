import ctypes

SYS_gettid = 186

class NotAvail:
    pass

class LibC:
    _libc = None

    def _load_lib(self):
        if self._libc:
            return
        self._libc = ctypes.cdll.LoadLibrary("libc.so.6")

    def syscall(self, id):
        self._load_lib()
        return self._libc.syscall(id)

class LibCap:
    _libcap = None
    pr_set_name = 15

    def _load_lib(self):
        if self._libcap:
            return
        try:
            self._libcap = ctypes.cdll.LoadLibrary("libcap.so.2")
        except OSError:
            self._libcap = NotAvail

    def tname(self, name):
        self._load_lib()
        if self._libcap is NotAvail:
            return
        self._libcap.prctl(self.pr_set_name, name.encode())

libc = LibC()
libcap = LibCap()

def get_tid():
    return libc.syscall(SYS_gettid)

def set_tname(name):
    libcap.tname(name)
