"""
Disk Heartbeat
"""
import sys
import os
import mmap
import stat
import errno
import contextlib
import json
import time

import daemon.shared as shared
import core.exceptions as ex
from env import Env
from .hb import Hb
from utilities.string import bdecode

class HbDisk(Hb):
    """
    A class factorizing common methods and properties for the disk
    heartbeat tx and rx child classes.
    """
    # 4MB meta size allows 1024 nodes (with 4k pagesize)
    METASIZE = 4 * 1024 * 1024

    # A 100MB disk can hold 96 nodes
    SLOTSIZE = 1024 * 1024

    MAX_SLOTS = METASIZE // mmap.PAGESIZE

    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "dev": self.dev,
            "timeout": self.timeout,
            "interval": self.interval,
        }
        for peer in data["peers"]:
            data["peers"][peer].update(self.peer_config.get(peer, {}))
        return data

    def configure(self):
        self.dev = None
        self.reset_stats()
        self._configure()

    def reconfigure(self):
        self._configure()

    def _configure(self):
        if self.name not in shared.NODE.cd:
            # this thread will be stopped. don't reconfigure to avoid logging errors
            return

        self.get_hb_nodes()
        self.peer_config = {}

        if not hasattr(self, "meta_slot_buff"):
            self.meta_slot_buff = mmap.mmap(-1, 2*mmap.PAGESIZE)
        if not hasattr(self, "slot_buff"):
            self.slot_buff = mmap.mmap(-1, self.SLOTSIZE)

        self.timeout = shared.NODE.oget(self.name, "timeout")
        self.interval = shared.NODE.oget(self.name, "interval")
        try:
            new_dev = shared.NODE.oget(self.name, "dev")
        except ex.RequiredOptNotFound:
            raise ex.AbortAction("no %s.dev is not set in node.conf" % self.name)

        if not os.path.exists(new_dev):
            raise ex.AbortAction("%s does not exist" % new_dev)

        new_dev = os.path.realpath(new_dev)
        new_flags = os.O_RDWR
        statinfo = os.stat(new_dev)
        if Env.sysname == "Linux":
            if stat.S_ISBLK(statinfo.st_mode):
                self.log.info("using directio")
                new_flags |= os.O_DIRECT | os.O_SYNC | os.O_DSYNC  # (Darwin, SunOS) pylint: disable=no-member
            else:
                raise ex.AbortAction("%s must be a block device" % new_dev)
        else:
            if not stat.S_ISCHR(statinfo.st_mode):
                raise ex.AbortAction("%s must be a char device" % new_dev)

        if new_dev != self.dev:
            self.dev = new_dev
            self.flags = new_flags
            self.peer_config = {}
            self.log.info("set dev=%s", self.dev)

        with self.hb_fo() as fo:
            self.load_peer_config(fo=fo)

    @contextlib.contextmanager
    def hb_fo(self):
        try:
            fd = os.open(self.dev, self.flags)
            fo = os.fdopen(fd, 'rb+')
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise ex.AbortAction("%s directio is not supported" % self.dev)
            else:
                raise ex.AbortAction("error opening %s: %s" % (self.dev, str(exc)))
        except Exception as exc:
            raise ex.AbortAction("error opening %s: %s" % (self.dev, str(exc)))
        try:
            yield fo
        except Exception as exc:
            self.log.error("%s: %s", self.dev, exc)
        finally:
            # closing fo also closes fd
            try:
                os.fsync(fd)
            except OSError as exc:
                self.duplog("error", "%(exc)s", exc=str(exc), nodename="")
            fo.close()

    @staticmethod
    def meta_slot_offset(slot):
        return slot * mmap.PAGESIZE

    def meta_read_slot(self, slot, fo=None):
        offset = self.meta_slot_offset(slot)
        fo.seek(offset, os.SEEK_SET)
        fo.readinto(self.meta_slot_buff)
        try:
            return bdecode(self.meta_slot_buff[:mmap.PAGESIZE])
        except Exception as exc:
            return None

    def meta_write_slot(self, slot, data, fo=None):
        if len(data) > mmap.PAGESIZE:
            self.log.error("attempt to write too long data in meta slot %d", slot)
            raise ex.AbortAction()
        self.meta_slot_buff.seek(0)
        self.meta_slot_buff.write(data)
        offset = self.meta_slot_offset(slot)
        fo.seek(offset, os.SEEK_SET)
        fo.write(self.meta_slot_buff)
        fo.flush()

    def slot_offset(self, slot):
        return self.METASIZE + slot * self.SLOTSIZE

    def read_slot(self, slot, fo=None):
        offset = self.slot_offset(slot)
        fo.seek(offset, os.SEEK_SET)
        fo.readinto(self.slot_buff)
        data = bdecode(self.slot_buff[:])
        end = data.index("\0")
        return data[:end]

    def write_slot(self, slot, data, fo=None):
        if len(data) > self.SLOTSIZE:
            self.log.error("attempt to write too long data in slot %d", slot)
            raise ex.AbortAction()
        self.slot_buff.seek(0)
        self.slot_buff.write(data)
        offset = self.slot_offset(slot)
        fo.seek(offset, os.SEEK_SET)
        fo.write(self.slot_buff)
        fo.flush()

    def load_peer_config(self, fo=None, verbose=True):
        for nodename in self.hb_nodes:
            if nodename not in self.peer_config:
                self.peer_config[nodename] = {
                    "slot": -1,
                }
        for slot in range(self.MAX_SLOTS):
            buff = self.meta_read_slot(slot, fo=fo)
            if buff is None or buff[0] == "\0":
                return
            try:
                nodename = buff[:buff.index("\0")]
            except IndexError:
                continue
            if nodename not in self.peer_config:
                continue
            if self.peer_config[nodename]["slot"] >= 0 and \
               slot != self.peer_config[nodename]["slot"]:
                if verbose:
                    self.log.warning("duplicate slot %d for node %s (first %d)",
                                     slot, nodename,
                                     self.peer_config[nodename]["slot"])
                continue
            if verbose:
                self.log.info("detect slot %d for node %s", slot, nodename)
            self.peer_config[nodename]["slot"] = slot

    def allocate_slot(self):
        for slot in range(self.MAX_SLOTS):
            with self.hb_fo() as fo:
                buff = self.meta_read_slot(slot, fo=fo)
                if buff is None or buff[0] != "\0":
                    continue
                self.peer_config[Env.nodename]["slot"] = slot
                try:
                    nodename = bytes(Env.nodename, "utf-8")
                except TypeError:
                    nodename = Env.nodename
                self.meta_write_slot(slot, nodename, fo=fo)
                self.log.info("allocated slot %d", slot)
            break


class HbDiskTx(HbDisk):
    """
    The disk heartbeat tx class.
    """
    def __init__(self, name):
        HbDisk.__init__(self, name, role="tx")

    def _configure(self):
        HbDisk._configure(self)
        if self.peer_config[Env.nodename]["slot"] < 0:
            self.allocate_slot()

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)

        try:
            while True:
                self.do()
                if self.stopped():
                    sys.exit(0)
                with shared.HB_TX_TICKER:
                    shared.HB_TX_TICKER.wait(self.interval)
        except Exception as exc:
            self.log.exception(exc)

    def do(self):
        self.janitor_procs()
        with self.hb_fo() as fo:
            self._do(fo)

    def _do(self, fo):
        self.reload_config()
        if Env.nodename not in self.peer_config:
            return
        slot = self.peer_config[Env.nodename]["slot"]
        if slot < 0:
            return
        message, message_bytes = self.get_message()
        if message is None:
            return

        data = (json.dumps({
            "msg": message,
            "updated": time.time(),
        })+'\0').encode()
        try:
            self.write_slot(slot, data, fo=fo)
            self.set_last()
            self.push_stats(message_bytes)
            #self.log.info("written to %s slot %s", self.dev, slot)
        except Exception as exc:
            self.push_stats()
            if self.get_last().success:
                self.log.error("write to %s slot %d error: %s", self.dev,
                               self.peer_config[Env.nodename]["slot"], exc)
            self.set_last(success=False)
        finally:
            self.set_beating()

class HbDiskRx(HbDisk):
    """
    The disk heartbeat rx class.
    """
    def __init__(self, name):
        HbDisk.__init__(self, name, role="rx")
        self.last_updated = {}

    def run(self):
        self.set_tid()
        try:
            self.configure()
        except ex.AbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)

        loop = 0
        while True:
            loop += 1
            if loop > 5:
                loop = 0
                missing = self.missing_peers()
                if missing:
                    self.log.info("reload slots for missing peers: %s", ", ".join(missing))
                    with self.hb_fo() as fo:
                        self.load_peer_config(fo=fo)
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.interval)

    def missing_peers(self):
        missing = []
        for nodename in self.hb_nodes:
            try:
                slot = self.peer_config[nodename]["slot"]
            except KeyError:
                missing.append(nodename)
                continue
            if slot < 0:
                missing.append(nodename)
        return missing

    def do(self):
        self.janitor_procs()
        with self.hb_fo() as fo:
            self._do(fo)

    def _do(self, fo):
        self.reload_config()
        for nodename, data in self.peer_config.items():
            if nodename == Env.nodename:
                continue
            if data["slot"] < 0:
                continue
            try:
                slot_data = json.loads(self.read_slot(data["slot"], fo=fo))
                _clustername, _nodename, _data = self.decrypt(slot_data["msg"])
                if _clustername != self.cluster_name:
                    continue
                if _nodename is None:
                    # invalid crypt
                    #self.log.warning("can't decrypt data in node %s slot",
                    #                 nodename)
                    continue
                if _nodename != nodename:
                    self.log.warning("node %s has written its data in node %s "
                                     "reserved slot", _nodename, nodename)
                    nodename = _nodename
                updated = slot_data["updated"]
                last_updated = self.last_updated.get(nodename)
                if last_updated is not None and last_updated == updated:
                    # remote tx has not rewritten its slot
                    #self.log.info("node %s has not updated its slot", nodename)
                    continue
                if updated < time.time() - self.timeout:
                    # discard too old dataset
                    continue
                self.last_updated[nodename] = updated
                self.queue_rx_data(_data, nodename)
                self.push_stats(len(slot_data))
                self.set_last(nodename)
            except Exception as exc:
                self.push_stats()
                if self.get_last(nodename).success:
                    self.log.error("read from %s slot %d (%s) error: %s", self.dev,
                                   data["slot"], nodename, str(exc))
                self.set_last(nodename, success=False)
            finally:
                self.set_beating(nodename)



