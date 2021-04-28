"""
Relay Heartbeat
"""
import sys
import os

import daemon.shared as shared
import core.exceptions as ex
from env import Env
from .hb import Hb

class HbRelay(Hb):
    """
    A class factorizing common methods and properties for the relay
    heartbeat tx and rx child classes.
    """
    def status(self, **kwargs):
        data = Hb.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "timeout": self.timeout,
            "interval": self.interval,
        }
        if hasattr(self, "relay"):
            data["config"]["relay"] = self.relay
        return data

    def configure(self):
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
        self.timeout = shared.NODE.oget(self.name, "timeout")
        self.interval = shared.NODE.oget(self.name, "interval")
        try:
            self.relay = shared.NODE.oget(self.name, "relay")
        except Exception:
            raise ex.AbortAction("no %s.relay is not set in node.conf" % self.name)
        try:
            self.secret = shared.NODE.oget(self.name, "secret")
        except Exception:
            raise ex.AbortAction("no %s.secret is not set in node.conf" % self.name)

class HbRelayTx(HbRelay):
    """
    The relay heartbeat tx class.
    """
    def __init__(self, name):
        HbRelay.__init__(self, name, role="tx")

    def _configure(self):
        HbRelay._configure(self)

    def run(self):
        self.set_tid()
        self.flags = os.O_RDWR
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

    def status(self, **kwargs):
        data = HbRelay.status(self, **kwargs)
        data["config"] = {}
        return data

    def do(self):
        self.janitor_procs()
        self.reload_config()
        message, message_bytes = self.get_message()
        if message is None:
            return

        try:
            self.send(message)
            self.set_last()
            self.push_stats(message_bytes)
            #self.log.info("written to %s slot %s", self.dev, slot)
        except Exception as exc:
            self.push_stats()
            if self.get_last().success:
                self.log.error("send to relay error: %s", exc)
            self.set_last(success=False)
        finally:
            self.set_beating()

    def send(self, message):
        request = {
            "action": "relay_tx",
            "options": {
                "cluster_name": self.cluster_name,
                "cluster_id": self.cluster_id,
                "msg": message,
            },
        }
        resp = self.daemon_post(request, cluster_name="join", server="raw://"+self.relay, secret=self.secret)
        if resp is None:
            raise ex.Error("not responding")
        if resp.get("status", 1) != 0:
            raise ex.Error("return status not 0")



class HbRelayRx(HbRelay):
    """
    The relay heartbeat rx class.
    """
    def __init__(self, name):
        HbRelay.__init__(self, name, role="rx")
        self.last_updated = {}

    def run(self):
        self.set_tid()
        self.flags = os.O_RDWR
        try:
            self.configure()
        except ex.AbortAction as exc:
            self.log.error(exc)
            self.stop()
            sys.exit(1)
        self.log.info("receive from relay %s", self.relay)

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            with shared.HB_TX_TICKER:
                shared.HB_TX_TICKER.wait(self.interval)

    def do(self):
        self.janitor_procs()
        self.reload_config()
        for nodename in self.hb_nodes:
            if nodename == Env.nodename:
                continue
            try:
                updated, slot_data = self.receive(nodename)
                _clustername, _nodename, _data = self.decrypt(slot_data, sender_id=self.relay)
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
                last_updated = self.last_updated.get(nodename)
                if last_updated is not None and last_updated == updated:
                    # remote tx has not rewritten its slot
                    #self.log.info("node %s has not updated its slot", nodename)
                    continue
                self.last_updated[nodename] = updated
                self.queue_rx_data(_data, nodename)
                self.push_stats(len(_data))
                self.set_last(nodename)
            except Exception as exc:
                self.push_stats()
                if self.get_last(nodename).success:
                    self.log.error("read from relay %s slot %s error: %s", self.relay,
                                   nodename, str(exc))
                self.set_last(nodename, success=False)
            finally:
                self.set_beating(nodename)

    def receive(self, nodename):
        request = {
            "action": "relay_rx",
            "options": {
                "slot": nodename,
                "cluster_id": self.cluster_id,
            },
        }
        resp = self.daemon_get(request, cluster_name="join", server="raw://"+self.relay, secret=self.secret)
        if resp is None:
            raise ex.Error("no response reading relay slot %s" % nodename)
        if resp.get("status", 1) != 0:
            raise ex.Error("return status not 0 reading relay slot %s" % nodename)
        if resp.get("data") is None:
            raise ex.Error("no data in response reading relay slot %s" % nodename)
        if resp.get("updated") is None:
            raise ex.Error("no 'updated' key in response reading relay slot %s" % nodename)
        try:
            # python3
            return resp.get("updated"), bytes(resp["data"], "ascii")
        except TypeError:
            return resp.get("updated"), resp["data"]

