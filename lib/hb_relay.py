"""
Relay Heartbeat
"""
import sys
import os
import errno
import contextlib

import osvcd_shared as shared
import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from comm import Crypt
from hb import Hb

class HbRelay(Hb, Crypt):
    """
    A class factorizing common methods and properties for the relay
    heartbeat tx and rx child classes.
    """
    DEFAULT_TIMEOUT = 15

    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data.stats = Storage(self.stats)
        data.config = {
            "timeout": self.timeout,
        }
        if hasattr(self, "relay"):
            data.config["relay"] = self.relay
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
            self.timeout = self.DEFAULT_TIMEOUT

        try:
            self.relay = self.config.get(self.name, "relay")
        except Exception:
            raise ex.excAbortAction("no %s.relay is not set in node.conf" % self.name)
        try:
            self.secret = self.config.get(self.name, "secret")
        except Exception:
            raise ex.excAbortAction("no %s.secret is not set in node.conf" % self.name)

class HbRelayTx(HbRelay):
    """
    The relay heartbeat tx class.
    """
    def __init__(self, name):
        HbRelay.__init__(self, name, role="tx")

    def _configure(self):
        HbRelay._configure(self)

    def run(self):
        self.flags = os.O_RDWR
        try:
            self.configure()
        except ex.excAbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)

        try:
            while True:
                self.do()
                if self.stopped():
                    sys.exit(0)
                with shared.HB_TX_TICKER:
                    shared.HB_TX_TICKER.wait(self.default_hb_period)
        except Exception as exc:
            self.log.exception(exc)

    def status(self, **kwargs):
        data = HbRelay.status(self, **kwargs)
        data["config"] = {}
        return data

    def do(self):
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        try:
            self.send(message)
            self.set_last()
            self.stats.beats += 1
            self.stats.bytes += message_bytes
            #self.log.info("written to %s slot %s", self.dev, slot)
        except Exception as exc:
            self.stats.errors += 1
            self.log.error("send to relay error: %s", exc)
            return
        finally:
            self.set_beating()

    def send(self, message):
        request = {
            "action": "relay_tx",
            "options": {
                "msg": message[:-1].decode("ascii"),
            },
        }
        resp = self.daemon_send(request, cluster_name="join", nodename=self.relay, secret=self.secret)
        if resp is None:
            raise ex.excError("not responding")
        if resp.get("status", 1) != 0:
            raise ex.excError("return status not 0")



class HbRelayRx(HbRelay):
    """
    The relay heartbeat rx class.
    """
    def __init__(self, name):
        HbRelay.__init__(self, name, role="rx")
        self.last_updated = {}

    def run(self):
        self.flags = os.O_RDWR
        try:
            self.configure()
        except ex.excAbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)
        self.log.info("receive from relay %s", self.relay)

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.default_hb_period)

    def do(self):
        self.reload_config()
        for nodename in self.cluster_nodes:
            if nodename == rcEnv.nodename:
                continue
            try:
                slot_data = self.receive(nodename)
                _nodename, _data = self.decrypt(slot_data)
                if _nodename is None:
                    # invalid crypt
                    #self.log.warning("can't decrypt data in node %s slot",
                    #                 nodename)
                    continue
                if _nodename != nodename:
                    self.log.warning("node %s has written its data in node %s "
                                     "reserved slot", _nodename, nodename)
                    nodename = _nodename
                updated = _data["updated"]
                last_updated = self.last_updated.get(nodename)
                if last_updated is not None and last_updated == updated:
                    # remote tx has not rewritten its slot
                    #self.log.info("node %s has not updated its slot", nodename)
                    continue
                self.last_updated[nodename] = updated
                with shared.CLUSTER_DATA_LOCK:
                    shared.CLUSTER_DATA[nodename] = _data
                self.stats.beats += 1
                self.set_last(nodename)
            except Exception as exc:
                self.stats.errors += 1
                self.log.error("read from relay %s slot %s error: %s", self.relay,
                               nodename, str(exc))
                return
            finally:
                self.set_beating(nodename)

    def receive(self, nodename):
        request = {
            "action": "relay_rx",
            "options": {
                "slot": nodename,
            },
        }
        resp = self.daemon_send(request, cluster_name="join", nodename=self.relay, secret=self.secret)
        if resp is None:
            raise ex.excError("not responding")
        if resp.get("status", 1) != 0:
            raise ex.excError("return status not 0")
        if resp.get("data") is None:
            raise ex.excError("no data in response")
        try:
            # python3
            return bytes(resp["data"], "ascii")
        except TypeError:
            return resp["data"]



