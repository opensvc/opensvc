import time
import uuid
from copy import deepcopy

import daemon.shared as shared
from env import Env

class LockMixin(object):
    """
    Methods shared between lock/unlock handlers.
    """
    def lock_acquire(self, nodename, name, timeout=None, thr=None):
        if timeout is None:
            timeout = 10
        if not nodename:
            nodename = Env.nodename
        elif nodename not in thr.cluster_nodes:
            return
        lock_id = None
        deadline = time.time() + timeout
        situation = 0
        while time.time() < deadline:
            if not lock_id:
                lock_id = self._lock_acquire(nodename, name)
                if not lock_id:
                    if situation != 1:
                        thr.log.info("claim %s lock refused (already claimed)", name)
                    situation = 1
                    time.sleep(0.5)
                    continue
                thr.log.info("claimed %s lock: %s", name, lock_id)
            if shared.LOCKS.get(name, {}).get("id") != lock_id:
                thr.log.info("claim %s dropped", name)
                lock_id = None
                continue
            if self.lock_accepted(name, lock_id):
                thr.log.info("locked %s", name)
                return lock_id
            time.sleep(0.5)
        thr.log.warning("claim timeout on %s lock", name)
        self.lock_release(name, lock_id, silent=True, thr=thr)

    def lock_release(self, name, lock_id, timeout=None, silent=False, thr=None):
        released = False
        if timeout is None:
            timeout = 5
        deadline = time.time() + timeout
        with shared.LOCKS_LOCK:
            if not lock_id or shared.LOCKS.get(name, {}).get("id") != lock_id:
                return
            del shared.LOCKS[name]
        shared.wake_monitor(reason="unlock", immediate=True)
        if not silent:
            thr.log.info("released locally %s", name)
        while time.time() < deadline:
            if self._lock_released(name, lock_id):
                released = True
                break
            time.sleep(0.5)
        if released is False:
            thr.log.warning('timeout waiting for lock %s %s release on peers', name, lock_id)

    def lock_accepted(self, name, lock_id):
        for nodename in self.list_nodes():
            try:
                lock = self.nodes_data.get([nodename, "locks", name])
            except KeyError:
                return False
            if lock.get("id") != lock_id:
                return False
        return True

    def _lock_released(self, name, lock_id):
        """
        Verify if lock release has been written to cluster data.
        """
        for nodename in self.list_nodes():
            try:
                lock = self.nodes_data.get([nodename, "locks", name])
            except KeyError:
                continue
            if lock.get("id") == lock_id:
                return False
        return True

    def _lock_acquire(self, nodename, name):
        with shared.LOCKS_LOCK:
            if name in shared.LOCKS:
                return
            lock_id = str(uuid.uuid4())
            shared.LOCKS[name] = {
                "requested": time.time(),
                "requester": nodename,
                "id": lock_id,
            }
        shared.wake_monitor(reason="lock", immediate=True)
        return lock_id

    def locks(self):
        return deepcopy(shared.LOCKS)
