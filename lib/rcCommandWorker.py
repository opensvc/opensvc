import os
from rcGlobalEnv import rcEnv

try:
    from multiprocessing import Queue, Process
    from Queue import Empty
    mp = True
    if rcEnv.sysname == "Windows":
        import sys
        from multiprocessing import set_executable
        set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
except:
    mp = False

from rcUtilities import justcall
import rcExceptions as ex

import logging
import logging.handlers
logfile = os.path.join(rcEnv.pathlog, 'cmdworker.log')
fileformatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
filehandler = logging.handlers.RotatingFileHandler(os.path.join(logfile),
                                                   maxBytes=5242880,
                                                   backupCount=5)
filehandler.setFormatter(fileformatter)
log = logging.getLogger("cmdworker")
log.addHandler(filehandler)
log.setLevel(logging.DEBUG)
log.debug("logger setup")

def worker(q):
    try:
        _worker(q)
    except ex.excSignal:
         log.info("interrupted by signal")

def _worker(q):
    log.debug("worker started")
    cmd = "foo"
    while cmd is not None:
        cmd = q.get()
        if cmd is None:
            log.debug("shutdown (poison pill)")
            break
        log.info("call: %s", ' '.join(cmd))
        out, err, ret = justcall(cmd)
        log.info("ret: %d", ret)
        continue
    log.debug("shutdown")

class CommandWorker(object):
    def __init__(self, name=""):
        self.q = None
        self.p = None
        self.name = "cmdworker"
        if name:
            self.name += "_"+name

    def start_worker(self):
        if not mp:
            return
        self.q = Queue()
        self.p = Process(target=worker, name=self.name, args=(self.q,))
        self.p.start()

    def enqueue(self, cmd):
        if not mp:
            out, err, ret = justcall(cmd)
            log.info("ret: %d", ret)
            return
        if self.p is None:
            self.start_worker()
        self.q.put(cmd, block=True)

    def stop_worker(self):
        if not mp:
            return
        if self.p is None or not self.p.is_alive():
            return
        log.debug("give poison pill to worker")
        self.enqueue(None)
        self.p.join()

