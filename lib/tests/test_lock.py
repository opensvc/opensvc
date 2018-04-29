import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import lock

class TestLock:
    def test_lock(self):
        """
        Locking
        """
        fpath = "/tmp/test.lock"

        def inline_lock():
            return lock.lock(lockfile=fpath, timeout=0, intent="test")

        def worker():
            import sys
            try:
                sys.exit(inline_lock())
            except lock.LockTimeout:
                sys.exit(255)

        def proc_lock():
            from multiprocessing import Process
            proc = Process(target=worker)
            proc.start()
            proc.join()
            return proc.exitcode

        lockfd = inline_lock()
        assert lockfd > 0

        # already lock owner
        relockfd = inline_lock()
        assert relockfd == None

        # lock conflict
        assert proc_lock() == 255

        # release
        lock.unlock(lockfd)

    def test_timeout_exc(self):
        """
        LockTimeOut exception
        """
        try:
            raise lock.LockTimeout(intent="test", pid=20000)
        except lock.LockTimeout as exc:
            assert exc.intent == "test"
            assert exc.pid == 20000

    def test_acquire_exc(self):
        """
        LockAcquire exception
        """
        try:
            raise lock.LockAcquire(intent="test", pid=20000)
        except lock.LockAcquire as exc:
            assert exc.intent == "test"
            assert exc.pid == 20000

