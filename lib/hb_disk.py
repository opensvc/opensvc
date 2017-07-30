"""
Disk Heartbeat
"""
import sys
import os
import mmap
import stat
import errno
import contextlib

import osvcd_shared as shared
import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from comm import Crypt
from hb import Hb

class HbDisk(Hb, Crypt):
    """
    A class factorizing common methods and properties for the disk
    heartbeat tx and rx child classes.
    """
    # 4MB meta size allows 1024 nodes (with 4k pagesize)
    METASIZE = 4 * 1024 * 1024

    # A 100MB disk can hold 96 nodes
    SLOTSIZE = 1024 * 1024

    MAX_SLOTS = METASIZE // mmap.PAGESIZE
    DEFAULT_DISK_TIMEOUT = 15

    def status(self):
        data = Hb.status(self)
        data.stats = Storage(self.stats)
        data.config = {
            "dev": self.dev,
            "timeout": self.timeout,
        }
        return data

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        self._configure()

    def reconfigure(self):
        self._configure()

    def _configure(self):
        self.peer_config = {}
        if hasattr(self, "node"):
            config = self.node.config
        else:
            config = self.config

        # timeout
        if self.config.has_option(self.name, "timeout@"+rcEnv.nodename):
            self.timeout = self.config.getint(self.name, "timeout@"+rcEnv.nodename)
        elif self.config.has_option(self.name, "timeout"):
            self.timeout = self.config.getint(self.name, "timeout")
        else:
            self.timeout = self.DEFAULT_DISK_TIMEOUT

        try:
            self.dev = self.config.get(self.name, "dev")
        except Exception:
            raise ex.excAbortAction("no %s.dev is not set in node.conf" % self.name)

        if not os.path.exists(self.dev):
            raise ex.excAbortAction("no %s does not exist" % self.dev)

        statinfo = os.stat(self.dev)
        if rcEnv.sysname == "Linux":
            if stat.S_ISBLK(statinfo.st_mode):
                self.log.info("using directio")
                self.flags |= os.O_DIRECT | os.O_SYNC
            else:
                raise ex.excAbortAction("%s must be a block device" % self.dev)
        else:
            if not stat.S_ISCHR(statinfo.st_mode):
                raise ex.excAbortAction("%s must be a char device" % self.dev)

        with self.dio_mm() as mm:
            self.load_peer_config(mm=mm)

    @contextlib.contextmanager
    def dio_mm(self):
        try:
            fd = os.open(self.dev, self.flags)
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise ex.excAbortAction("%s directio is not supported" % self.dev)
            else:
                raise ex.excAbortAction("error opening %s: %s" % (self.dev, str(exc)))
        except Exception as exc:
            raise ex.excAbortAction("error opening %s: %s" % (self.dev, str(exc)))

        mmap_kwargs = {}
        if rcEnv.sysname != "Windows" and not self.flags & os.O_RDWR:
            #self.log.info("mmap %s set read protection", self.dev)
            mmap_kwargs["prot"] = mmap.PROT_READ
        try:
            size = os.lseek(fd, 0, os.SEEK_END)
            os.lseek(fd, 0, os.SEEK_SET)
            #self.log.debug("%s size %d", self.dev, size)
            mm = mmap.mmap(fd, size, **mmap_kwargs)
            yield mm
        except Exception as exc:
            raise ex.excAbortAction("mmapping %s: %s" % (self.dev, str(exc)))
        finally:
            mm.close()
            os.close(fd)

    @staticmethod
    def meta_slot_offset(slot):
        return slot * mmap.PAGESIZE

    def meta_read_slot(self, slot, mm=None):
        offset = self.meta_slot_offset(slot)
        return mm[offset:offset+mmap.PAGESIZE]

    def meta_write_slot(self, slot, data, mm=None):
        if len(data) > mmap.PAGESIZE:
            self.log.error("attempt to write too long data in meta slot %d", slot)
            raise ex.excAbortAction()
        offset = self.meta_slot_offset(slot)
        mm.seek(offset)
        mm.write(data)
        mm.flush()

    def slot_offset(self, slot):
        return self.METASIZE + slot * self.SLOTSIZE

    def read_slot(self, slot, mm=None):
        offset = self.slot_offset(slot)
        end = mm[offset:offset+self.SLOTSIZE].index("\0")
        return mm[offset:offset+end]

    def write_slot(self, slot, data, mm=None):
        if len(data) > self.SLOTSIZE:
            self.log.error("attempt to write too long data in slot %d", slot)
            raise ex.excAbortAction()
        offset = self.slot_offset(slot)
        mm.seek(offset)
        mm.write(data)
        mm.flush()

    def load_peer_config(self, mm=None, verbose=True):
        for nodename in self.cluster_nodes:
            if nodename not in self.peer_config:
                self.peer_config[nodename] = Storage({
                    "slot": -1,
                })
        for slot in range(self.MAX_SLOTS):
            buff = self.meta_read_slot(slot, mm=mm)
            if buff[0] == "\0":
                return
            nodename = buff.strip("\0")
            if nodename not in self.peer_config:
                continue
            if self.peer_config[nodename].slot >= 0:
                if verbose:
                    self.log.warning("duplicate slot %d for node %s (first %d)",
                                     slot, nodename,
                                     self.peer_config[nodename].slot)
                continue
            if verbose:
                self.log.info("detect slot %d for node %s", slot, nodename)
            self.peer_config[nodename]["slot"] = slot

    def allocate_slot(self):
        for slot in range(self.MAX_SLOTS):
            with self.dio_mm() as mm:
                buff = self.meta_read_slot(slot, mm=mm)
                if buff[0] != "\0":
                    continue
                self.peer_config[rcEnv.nodename].slot = slot
                self.meta_write_slot(slot, rcEnv.nodename, mm=mm)
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

    def run(self):
        self.flags = os.O_RDWR
        try:
            self.configure()
            if self.peer_config[rcEnv.nodename].slot < 0:
                self.allocate_slot()
        except ex.excAbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.default_hb_period)

    def status(self):
        data = HbDisk.status(self)
        data["config"] = {}
        return data

    def do(self):
        with self.dio_mm() as mm:
            self._do(mm)

    def _do(self, mm):
        if rcEnv.nodename not in self.peer_config:
            return
        slot = self.peer_config[rcEnv.nodename].slot
        if slot < 0:
            return
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        try:
            self.write_slot(slot, message, mm=mm)
            self.set_last()
            self.stats.beats += 1
            self.stats.bytes += message_bytes
            #self.log.info("written to %s slot %s", self.dev, slot)
        except Exception as exc:
            self.stats.errors += 1
            self.log.error("write to %s slot %d error: %s", self.dev,
                           self.peer_config[rcEnv.nodename]["slot"], exc)
            return
        finally:
            self.set_beating()

class HbDiskRx(HbDisk):
    """
    The disk heartbeat rx class.
    """
    def __init__(self, name):
        HbDisk.__init__(self, name, role="rx")
        self.last_updated = None

    def run(self):
        self.flags = os.O_RDONLY
        try:
            self.configure()
        except ex.excAbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)
        self.log.info("reading on %s", self.dev)

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.default_hb_period)

    def do(self):
        with self.dio_mm() as mm:
            self._do(mm)

    def _do(self, mm):
        self.reload_config()
        self.load_peer_config(verbose=False, mm=mm)
        for nodename, data in self.peer_config.items():
            if nodename == rcEnv.nodename:
                continue
            if data.slot < 0:
                continue
            try:
                slot_data = self.read_slot(data.slot, mm=mm)
                _nodename, _data = self.decrypt(slot_data)
                if _nodename is None:
                    # invalid crypt
                    continue
                if _nodename != nodename:
                    self.log.warning("node %s has written its data in node %s "
                                     "reserved slot", _nodename, nodename)
                    nodename = _nodename
                updated = _data["updated"]
                if self.last_updated is not None and self.last_updated == updated:
                    # remote tx has not rewritten its slot
                    continue
                self.last_updated = updated
                with shared.CLUSTER_DATA_LOCK:
                    shared.CLUSTER_DATA[nodename] = _data
                self.stats.beats += 1
                self.set_last(nodename)
            except Exception as exc:
                self.stats.errors += 1
                self.log.error("read from %s slot %d (%s) error: %s", self.dev,
                               data.slot, nodename, str(exc))
                return
            finally:
                self.set_beating(nodename)



