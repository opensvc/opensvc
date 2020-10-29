import threading
import time

from traceback import format_stack

class _Lock(object):
    def __init__(self, o):
        self._lock = o()
        self.t = 0
        self.name = ""
        self.holder = None

    def acquire(self, *args, **kwargs):
        s = "".join(format_stack()[2:-3])
        if self.holder:
            print("=== %s acquire\n%s" % (self.name, s))
            print(">>> %s held by\n%s" % (self.name, self.holder))
        self._lock.acquire(*args, **kwargs)
        self.holder = s

    def release(self, *args, **kwargs):
        self.holder = None
        self._lock.release()

    def __enter__(self):
        s = "".join(format_stack()[2:-3])
        if self.holder:
            print("=== %s acquire\n%s" % (self.name, s))
            print(">>> %s held by\n%s" % (self.name, self.holder))
        self.t = time.time()
        self._lock.acquire()
        self.holder = s

    def __exit__(self, type, value, traceback):
        self._lock.release()
        self.holder = None
        d = time.time() - self.t
        if d < 1:
            return
        #print("=== %s held %.2fs\n%s" % (self.name, d, "".join(format_stack()[2:-1])))

class Lock(_Lock):
    def __init__(self):
         _Lock.__init__(self, threading.Lock)

class RLock(_Lock):
    def __init__(self):
         _Lock.__init__(self, threading.RLock)

