import fcntl
import os
import action as ex
from rcGlobalEnv import rcEnv

def svclock(svc):
    lock = os.path.join(rcEnv.pathlock, svc.svcname)
    if not os.path.exists(rcEnv.pathlock):
        os.makedirs(rcEnv.pathlock)
    try:
        svc.lockfd = os.open(lock, os.O_RDWR|os.O_SYNC|os.O_CREAT, 0600)
    except:
        svc.log.error("can not create lock file %s"%lock)
        raise

    try:
        fcntl.flock(svc.lockfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        os.fsync(svc.lockfd)
        os.write(svc.lockfd, str(os.getpid()))
    except IOError:
        fd = open(lock, 'r')
        pid = fd.read()
        svc.log.error("another action is currently running (pid=%s)"%pid)
        raise ex.excError
    except:
        raise

