"""
Implement a node-wide locking mechanism.
File-based, lock with fcntl exclusive open when available.
"""

from __future__ import print_function

import contextlib
import json
import os
import time

import foreign.six as six


class LockNoLockFile(Exception):
    """ no lockfile specified
    """


class LockCreateError(Exception):
    """ could not create lockfile
    """


class LockAcquire(Exception):
    """ could not acquire lock on lockfile
    """

    # noinspection PyShadowingNames
    def __init__(self, intent="", pid=0, progress=None, path=None):
        Exception.__init__(self)
        self.intent = intent
        self.pid = pid
        self.progress = progress
        self.path = path

    def __str__(self):
        s = "lock %(path)s holder pid %(pid)d, holder intent '%(intent)s'" % dict(pid=self.pid, intent=self.intent, path=self.path)
        if self.progress:
            s += ", progress '%s'" % str(self.progress)
        return s


class LockTimeout(LockAcquire):
    """ acquire lock timed out
    """


LOCK_EXCEPTIONS = (
    LockTimeout,
    LockNoLockFile,
    LockCreateError,
    LockAcquire,
)


def bencode(buff):
    """
    Try a bytes cast, which only work in python3.
    """
    try:
        return bytes(buff, "utf-8")
    except TypeError:
        return buff


def bdecode(buff):
    """
    On python, convert bytes to string using utf-8 and ascii as a fallback
    """
    if buff is None:
        return buff
    if six.PY2:
        return buff
    if type(buff) == str:
        return buff
    else:
        try:
            return str(buff, "utf-8")
        except:
            return str(buff, "ascii")


@contextlib.contextmanager
def cmlock(*args, **kwargs):
    """
    A context manager protecting a code path that can't run twice on the
    same node.
    """
    lockfd = None
    try:
        lockfd = lock(*args, **kwargs)
        yield lockfd
    finally:
        unlock(lockfd)


def lock(timeout=30, delay=1, lockfile=None, intent=None):
    """
    The lock acquire function.
    """
    if timeout == 0 or delay == 0:
        ticks = [0]
    else:
        ticks = range(int(float(timeout) / float(delay)))
    if len(ticks) == 0:
        ticks = [0]
    err = {}
    for tick in ticks:
        try:
            return lock_nowait(lockfile, intent)
        except LockAcquire as exc:
            err["intent"] = exc.intent
            err["pid"] = exc.pid
            err["path"] = exc.path
        except Exception:
            raise
        if tick > 0:
            time.sleep(delay)
    raise LockTimeout(**err)


def lock_nowait(lockfile=None, intent=None):
    """
    A lock acquire function variant without timeout not delay.
    """
    if lockfile is None:
        raise LockNoLockFile

    data = {"pid": os.getpid(), "intent": intent}
    lock_dir = os.path.dirname(lockfile)

    try:
        with open(lockfile, 'r') as ofile:
            prev_data = json.load(ofile)
        if not isinstance(prev_data, dict) or "pid" not in prev_data or "intent" not in prev_data:
            prev_data = {"pid": 0, "intent": ""}
    except Exception as exc:
        if hasattr(exc, "errno") and getattr(exc, "errno") == 21:
            raise LockCreateError("lockfile points to a directory")
        prev_data = {"pid": 0, "intent": ""}

    # test if we already own the lock
    if prev_data["pid"] == os.getpid():
        return

    flags = os.O_RDWR | os.O_CREAT
    if os.name == 'nt':
        flags |= os.O_TRUNC
    else:
        flags |= os.O_SYNC

    try:
        lockfd = os.open(lockfile, flags, 0o644)
    except Exception as exc:
        if hasattr(exc, "errno") and getattr(exc, "errno") == 2:
            os.makedirs(lock_dir)
            try:
                lockfd = os.open(lockfile, flags, 0o644)
            except Exception as exc:
                raise LockCreateError(str(exc))
        else:
            raise LockCreateError(str(exc))

    try:
        # FD_CLOEXEC makes sure the lock is the held by processes
        # we fork from this process
        if os.name == 'posix':
            import fcntl
            fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            flags = fcntl.fcntl(lockfd, fcntl.F_GETFD)
            flags |= fcntl.FD_CLOEXEC

            # acquire lock
            fcntl.fcntl(lockfd, fcntl.F_SETFD, flags)
        elif os.name == 'nt':
            try:
                # noinspection PyUnresolvedReferences
                import msvcrt
            except ImportError:
                raise
            msvcrt.locking(lockfd, msvcrt.LK_NBRLCK, 1)

        # drop our pid and intent in the lockfile, best effort
        try:
            if os.name == "posix":
                os.ftruncate(lockfd, 0)
            os.write(lockfd, bencode(json.dumps(data)))
            os.fsync(lockfd)
        except Exception:
            pass
        return lockfd
    except IOError:
        os.close(lockfd)
        raise LockAcquire(path=lockfile, **prev_data)
    except:
        os.close(lockfd)
        raise


def unlock(lockfd):
    """
    The lock release function.
    """
    if lockfd is None:
        return
    try:
        os.ftruncate(lockfd, 0)
        os.close(lockfd)
    except Exception:
        # already released by a parent process ?
        pass


def progress(lockfd, data):
    if lockfd is None:
        return
    try:
        _lockfd = os.dup(lockfd)
        with os.fdopen(_lockfd, "w+") as ofile:
            ofile.seek(0)
            try:
                lock_data = json.load(ofile)
            except ValueError:
                return
            lock_data["progress"] = data
            ofile.truncate(0)
            ofile.seek(0)
            json.dump(lock_data, ofile)
            os.fsync(_lockfd)
        os.close(_lockfd)
    except Exception:
        return


def main():
    """
    Expose the locking functions as a command line tool.
    """
    import optparse

    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", default="/tmp/test.lock", action="store",
                      dest="file", help="The file to lock")
    parser.add_option("-i", "--intent", default="test", action="store",
                      dest="intent", help="The lock intent")
    parser.add_option("-t", "--time", default=60, action="store", type="int",
                      dest="time", help="The time we will hold the lock")
    parser.add_option("--timeout", default=1, action="store", type="int",
                      dest="timeout",
                      help="The time before failing to acquire the lock")
    (options, _) = parser.parse_args()
    try:
        with cmlock(timeout=options.timeout, delay=1, lockfile=options.file,
                    intent=options.intent):
            print("lock acquired")
            try:
                time.sleep(options.time)
            except KeyboardInterrupt:
                pass
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
