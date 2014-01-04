import os
import time
import rcExceptions as ex
from rcGlobalEnv import rcEnv

class lockTimeout(Exception):
    """ acquire lock timed out
    """

class lockNoLockFile(Exception):
    """ no lockfile specified
    """

class lockCreateError(Exception):
    """ could not create lockfile
    """

class lockAcquire(Exception):
    """ could not acquire lock on lockfile
    """
    def __init__(self, pid):
        self.pid = pid

def monlock(timeout=30, delay=5, fname='svcmon.lock'):
    lockfile = os.path.join(rcEnv.pathlock, fname)
    try:
        lockfd = lock(timeout=timeout, delay=delay, lockfile=lockfile)
    except lockTimeout:
        print("timed out waiting for lock (%s)"%lockfile)
        raise ex.excError
    except lockNoLockFile:
        print("lock_nowait: set the 'lockfile' param")
        raise ex.excError
    except lockCreateError:
        print("can not create lock file (%s)"%lockfile)
        raise ex.excError
    except lockAcquire as e:
        print("another svcmon is currently running (pid=%s)"%e.pid)
        raise ex.excError
    except:
        print("unexpected locking error (%s)"%lockfile)
        raise ex.excError
    return lockfd

def monunlock(lockfd):
    unlock(lockfd)

def lock(timeout=30, delay=5, lockfile=None):
    for i in range(timeout//delay):
        try:
            return lock_nowait(lockfile)
        except lockAcquire:
            time.sleep(delay)
    raise lockTimeout

def lock_nowait(lockfile=None):
    if lockfile is None:
        raise lockNoLockFile

    pid = 0
    dir = os.path.dirname(lockfile)

    if not os.path.exists(dir):
        os.makedirs(dir)

    try:
        with open(lockfile, 'r') as fd:
            pid = int(fd.read())
            fd.close()
    except:
        pass

    try:
	flags = os.O_RDWR|os.O_CREAT|os.O_TRUNC
	if rcEnv.sysname != 'Windows':
	    flags |= os.O_SYNC
        lockfd = os.open(lockfile, flags, 0o644)
    except Exception as e:
        raise lockCreateError()

    try:
        """ test if we already own the lock
        """
        if pid == os.getpid():
            os.close(lockfd)
            return

        """ FD_CLOEXEC makes sure the lock is the held by processes
            we fork from this process
        """
        if os.name == 'posix':
            import fcntl
            fcntl.flock(lockfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
            flags = fcntl.fcntl(lockfd, fcntl.F_GETFD)
            flags |= fcntl.FD_CLOEXEC

            """ acquire lock
            """
            fcntl.fcntl(lockfd, fcntl.F_SETFD, flags)
        elif os.name == 'nt':
            import msvcrt
            size = os.path.getsize(lockfile)
            msvcrt.locking(lockfd, msvcrt.LK_RLCK, size)

        """ drop our pid in the lockfile
        """
        os.write(lockfd, str(os.getpid()).encode('utf-8'))
        os.fsync(lockfd)
        return lockfd
    except IOError:
        raise lockAcquire(pid)
    except:
        raise

def unlock(lockfd):
    if lockfd is None:
        return
    try:
        os.close(lockfd)
    except:
        """ already released by a parent process ?
        """
        pass

