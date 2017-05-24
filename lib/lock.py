from __future__ import print_function
import os
import time
import json
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

LOCK_EXCEPTIONS = (
    lockTimeout,
    lockNoLockFile,
    lockCreateError,
    lockAcquire,
)

def monlock(timeout=0, delay=0, fname='svcmon.lock'):
    lockfile = os.path.join(rcEnv.paths.pathlock, fname)
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

def lock(timeout=30, delay=1, lockfile=None, intent=None):
    if timeout == 0 or delay == 0:
        l = [0]
    else:
        l = range(int(timeout/delay))
    if len(l) == 0:
        l = [0]
    err = ""
    for i in l:
        if i > 0:
            time.sleep(delay)
        try:
            fd = lock_nowait(lockfile, intent)
            return fd
        except lockAcquire as e:
            err = str(e)
        except Exception:
            raise
    raise lockTimeout(err)

def lock_nowait(lockfile=None, intent=None):
    if lockfile is None:
        raise lockNoLockFile

    data = {"pid": os.getpid(), "intent": intent}
    dir = os.path.dirname(lockfile)

    if not os.path.exists(dir):
        os.makedirs(dir)

    try:
        with open(lockfile, 'r') as fd:
            buff = fd.read()
        prev_data = json.loads(buff)
        fd.close()
        #print("lock data from file", lockfile, prev_data)
        if type(prev_data) != dict or "pid" not in prev_data or "intent" not in prev_data:
            prev_data = {"pid": 0, "intent": ""}
            #print("lock data corrupted", lockfile, prev_data)
    except Exception as e:
        prev_data = {"pid": 0, "intent": ""}
        #print("error reading lockfile", lockfile, prev_data, str(e))

    """ test if we already own the lock
    """
    if prev_data["pid"] == os.getpid():
        return

    if os.path.isdir(lockfile):
        raise lockCreateError("lockfile points to a directory")

    try:
        flags = os.O_RDWR|os.O_CREAT
        if rcEnv.sysname != 'Windows':
            flags |= os.O_SYNC
        lockfd = os.open(lockfile, flags, 0o644)
    except Exception as e:
        raise lockCreateError(str(e))

    try:
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

        """ drop our pid and intent in the lockfile, best effort
        """
        fd = lockfd
        try:
            os.ftruncate(lockfd, 0)
            os.write(lockfd, json.dumps(data))
            os.fsync(lockfd)
        except:
            pass
        return fd
    except IOError:
        raise lockAcquire("holder pid %(pid)d, holder intent '%(intent)s'" % prev_data)
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


if __name__ == "__main__":
    import optparse
    import time
    import sys

    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", default="/tmp/test.lock", action="store", dest="file",
                  help="The file to lock")
    parser.add_option("-i", "--intent", default="test", action="store", dest="intent",
                  help="The lock intent")
    parser.add_option("-t", "--time", default=60, action="store", type="int", dest="time",
                  help="The time we will hold the lock")
    parser.add_option("--timeout", default=1, action="store", type="int", dest="timeout",
                  help="The time before failing to acquire the lock")
    (options, args) = parser.parse_args()
    try:
        lockfd = lock(timeout=options.timeout, delay=1, lockfile=options.file, intent=options.intent)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    print("lock acquired")
    try:
        time.sleep(options.time)
    except KeyboardInterrupt:
        pass

