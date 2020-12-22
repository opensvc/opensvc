import time
import uuid
from copy import deepcopy

import daemon.shared as shared
from env import Env

DELAY_TIME = 0.5


class LockMixin(object):
    """
    Methods shared between lock/unlock handlers.
    """
    def lock_acquire(self, nodename, name, timeout=None, thr=None):
        begin = time.time()
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
                lock_id = self._lock_acquire(nodename, name, thr=thr)
                if not lock_id:
                    if situation != 1:
                        thr.log.info("claim %s lock refused (already claimed)", name)
                    situation = 1
                    time.sleep(DELAY_TIME)
                    continue
                thr.log.info("claimed %s lock: %s", name, lock_id)
            if shared.LOCKS.get(name, {}).get("id") != lock_id:
                thr.log.info("claim %s dropped", name)
                lock_id = None
                continue
            if self.lock_accepted(name, lock_id, thr=thr):
                thr.log.info("acquire %s %s duration (%s)", name, lock_id, int(time.time()-begin))
                return lock_id
            time.sleep(DELAY_TIME)
        thr.log.warning("claim timeout on %s lock (duration %s s)", name, int(time.time()-begin))
        self.lock_release(name, lock_id, silent=True, thr=thr)

    def lock_release(self, name, lock_id, timeout=None, silent=False, thr=None):
        begin = time.time()
        released = False
        if timeout is None:
            timeout = 5
        deadline = time.time() + timeout
        with shared.LOCKS_LOCK:
            if not lock_id or shared.LOCKS.get(name, {}).get("id") != lock_id:
                return
            del shared.LOCKS[name]
            if thr:
                thr.update_cluster_locks_lk()
        shared.wake_monitor(reason="unlock", immediate=True)
        if not silent:
            thr.log.info("released locally %s", name)
        while time.time() < deadline:
            if self._lock_released(name, lock_id, thr=thr):
                released = True
                break
            time.sleep(DELAY_TIME)
        if released is False:
            thr.log.warning('timeout waiting for lock %s %s release on peers', name, lock_id)
        else:
            thr.log.info("lock_released on %s lock %s (duration %s s)", name, lock_id, int(time.time()-begin))

    def lock_accepted(self, name, lock_id, thr=None):
        for nodename in thr.list_nodes():
            try:
                lock = thr.nodes_data.get([nodename, "locks", name])
            except KeyError:
                thr.log.info('lock not yet held by %s (id %s)', nodename, lock_id)
                return False
            if lock.get("id") != lock_id:
                thr.log.info('lock is held by %s with id %s', nodename, lock.get("id"))
                return False
        return True

    def _lock_released(self, name, lock_id, thr=None):
        """
        Verify if lock release has been written to cluster data.
        """
        for nodename in thr.list_nodes():
            try:
                lock = thr.nodes_data.get([nodename, "locks", name])
            except KeyError:
                continue
            if lock.get("id") == lock_id:
                return False
        return True

    def _lock_acquire(self, nodename, name, thr=None):
        lock_id = str(uuid.uuid4())
        with shared.LOCKS_LOCK:
            if name in shared.LOCKS:
                return
            shared.LOCKS[name] = {
                "requested": time.time(),
                "requester": nodename,
                "id": lock_id,
            }
            if thr:
                thr.update_cluster_locks_lk()
        shared.wake_monitor(reason="lock", immediate=True)
        return lock_id

    def locks(self):
        return deepcopy(shared.LOCKS)
