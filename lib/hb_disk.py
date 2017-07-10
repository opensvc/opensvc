"""
Disk Heartbeat
"""
import sys
import os
import mmap
import stat
import errno

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

    def unconfigure(self):
        if hasattr(self, "mm"):
            self.mm.close()
        if hasattr(self, "fd"):
            os.close(self.fd)

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
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

        try:
            self.fd = os.open(self.dev, self.flags)
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise ex.excAbortAction("%s directio is not supported" % self.dev)
            else:
                raise ex.excAbortAction("error opening %s: %s" % (self.dev, str(exc)))
        except Exception as exc:
            raise ex.excAbortAction("error opening %s: %s" % (self.dev, str(exc)))

        mmap_kwargs = {}
        if rcEnv.sysname != "Windows" and not self.flags & os.O_RDWR:
            self.log.info("mmap %s set read protection", self.dev)
            mmap_kwargs["prot"] = mmap.PROT_READ
        try:
            size = os.lseek(self.fd, 0, os.SEEK_END)
            os.lseek(self.fd, 0, os.SEEK_SET)
            self.log.info("mmap %s size %d", self.dev, size)
            self.mm = mmap.mmap(self.fd, size, **mmap_kwargs)
        except Exception as exc:
            os.close(self.fd)
            raise ex.excAbortAction("mmapping %s: %s" % (self.dev, str(exc)))

        self.load_peer_config()

    @staticmethod
    def meta_slot_offset(slot):
        return slot * mmap.PAGESIZE

    def meta_read_slot(self, slot):
        offset = self.meta_slot_offset(slot)
        return self.mm[offset:offset+mmap.PAGESIZE]

    def meta_write_slot(self, slot, data):
        if len(data) > mmap.PAGESIZE:
            self.log.error("attempt to write too long data in meta slot %d", slot)
            raise ex.excAbortAction()
        offset = self.meta_slot_offset(slot)
        self.mm.seek(offset)
        self.mm.write(data)
        self.mm.flush()

    def slot_offset(self, slot):
        return self.METASIZE + slot * self.SLOTSIZE

    def read_slot(self, slot):
        offset = self.slot_offset(slot)
        end = self.mm[offset:offset+self.SLOTSIZE].index("\0")
        return self.mm[offset:offset+end]

    def write_slot(self, slot, data):
        if len(data) > self.SLOTSIZE:
            self.log.error("attempt to write too long data in slot %d", slot)
            raise ex.excAbortAction()
        offset = self.slot_offset(slot)
        self.mm.seek(offset)
        self.mm.write(data)
        self.mm.flush()

    def load_peer_config(self, verbose=True):
        for nodename in self.cluster_nodes:
            if nodename not in self.peer_config:
                self.peer_config[nodename] = Storage({
                    "slot": -1,
                })
        for slot in range(self.MAX_SLOTS):
            buff = self.meta_read_slot(slot)
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
            buff = self.meta_read_slot(slot)
            if buff[0] != "\0":
                continue
            self.peer_config[rcEnv.nodename].slot = slot
            self.meta_write_slot(slot, rcEnv.nodename)
            self.log.info("allocated slot %d", slot)
            break


class HbDiskTx(HbDisk):
    """
    The disk heartbeat tx class.
    """
    def __init__(self, name):
        HbDisk.__init__(self, name, role="tx")

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
                self.unconfigure()
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(shared.DEFAULT_HB_PERIOD)

    def status(self):
        data = HbDisk.status(self)
        data["config"] = {}
        return data

    def do(self):
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
            self.write_slot(slot, message)
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
                self.unconfigure()
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(shared.DEFAULT_HB_PERIOD)

    def do(self):
        self.reload_config()
        self.load_peer_config(verbose=False)
        for nodename, data in self.peer_config.items():
            if nodename == rcEnv.nodename:
                continue
            if data.slot < 0:
                continue
            try:
                slot_data = self.read_slot(data.slot)
                _nodename, _data = self.decrypt(slot_data)
                if _nodename is None:
                    # invalid crypt
                    continue
                if _nodename != nodename:
                    self.log.warning("node %s has written its data in node %s "
                                     "reserved slot", _nodename, nodename)
                    nodename = _nodename
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



