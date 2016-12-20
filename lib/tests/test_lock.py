import lock

def test_lock():
    fpath = "/tmp/test.lock"

    def inline_lock():
        return lock.lock(lockfile=fpath, timeout=0, intent="test")

    def worker():
        import sys
        try:
            sys.exit(inline_lock())
        except lock.lockTimeout:
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
