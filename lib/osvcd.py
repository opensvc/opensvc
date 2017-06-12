"""
The opensvc daemon.
"""
from __future__ import print_function

import sys
import os
import time
import datetime
import socket
import threading
from subprocess import Popen, PIPE
import logging
import json
import struct
import stat
import base64
import fcntl
import codecs
import hashlib
import zlib
import glob
from optparse import OptionParser

import rcExceptions as ex
import rcLogger
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import justcall, bdecode, lazy, unset_lazy
from rcStatus import Status
import pyaes

try:
    from Crypto.Cipher import AES
    def _encrypt(message, key, iv):
        message = pyaes.util.append_PKCS7_padding(zlib.compress(message.encode()))
        obj = AES.new(key, AES.MODE_CBC, iv)
        ciphertext = obj.encrypt(message)
        return ciphertext
    def _decrypt(ciphertext, key, iv):
        obj = AES.new(key, AES.MODE_CBC, iv)
        message = obj.decrypt(ciphertext)
        return zlib.decompress(pyaes.util.strip_PKCS7_padding(message))
except ImportError:
    def _encrypt(message, key, iv):
        obj = pyaes.Encrypter(pyaes.AESModeOfOperationCBC(key, iv=iv))
        ciphertext = obj.feed(zlib.compress(message.encode()))
        ciphertext += obj.feed()
        return ciphertext
    def _decrypt(ciphertext, key, iv):
        obj = pyaes.Decrypter(pyaes.AESModeOfOperationCBC(key, iv=iv))
        message = obj.feed(ciphertext)
        message += obj.feed()
        return zlib.decompress(message)

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


MON_WAIT_READY = datetime.timedelta(seconds=6)
DEFAULT_HB_PERIOD = 5
MAX_MSG_SIZE = 1024 * 1024
DATEFMT = "%Y-%m-%dT%H:%M:%S.%fZ"

# locked globals
THREADS = {}
THREADS_LOCK = threading.RLock()
CLUSTER_DATA = {}
CLUSTER_DATA_LOCK = threading.RLock()
HB_MSG = None
HB_MSG_LOCK = threading.RLock()
SERVICES = {}
SERVICES_LOCK = threading.RLock()
MON_DATA = {}
MON_DATA_LOCK = threading.RLock()


def fork(func, args=None, kwargs=None):
    """
    A fork daemonizing function.
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    if os.fork() > 0:
        # return to parent execution
        return

    # separate the son from the father
    os.chdir('/')
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except:
        os._exit(1)

    lockfile = "osvcd.lock"
    lockfile = os.path.join(rcEnv.paths.pathlock, lockfile)

    from lock import lock, unlock
    try:
        lockfd = lock(lockfile=lockfile, timeout=0, delay=0)
    except Exception:
        os._exit(0)

    # Redirect standard file descriptors.
    if (hasattr(os, "devnull")):
       devnull = os.devnull
    else:
       devnull = "/dev/null"

    for fd in range(0, 3):
        try:
            os.close(fd)
        except OSError:
            pass

    # Popen(close_fds=True) does not close 0, 1, 2. Make sure we have those
    # initialized to /dev/null
    os.open(devnull, os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)

    try:
        func(*args, **kwargs)
    except Exception as exc:
        unlock(lockfd)
        os._exit(1)

    unlock(lockfd)
    os._exit(0)

def forked(func):
    """
    A decorator that runs the decorated function in a detached subprocess
    immediately. A lock is held to avoid running the same function twice.
    """
    def _func(*args, **kwargs):
        fork(func, args, kwargs)
    return _func

#
class OsvcThread(threading.Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """
    stop_tmo = 60

    def __init__(self):
        super(OsvcThread, self).__init__()
        self._stop_event = threading.Event()
        self.created = time.time()
        self.threads = []
        self.procs = []

    def stop(self):
        self._stop_event.set()

    def unstop(self):
        self._stop_event.clear()

    def stopped(self):
        return self._stop_event.is_set()

    def status(self):
        if self.stopped():
            if self.is_alive():
                state = "stopping"
            else:
                state = "stopped"
        else:
            if self.is_alive():
                state = "running"
            else:
                state = "terminated"
        data = Storage({
                "state": state,
                "created": datetime.datetime.utcfromtimestamp(self.created).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
        return data

    def push_proc(self, proc,
                  on_success=None, on_success_args=None, on_success_kwargs=None,
                  on_error=None, on_error_args=None, on_error_kwargs=None):
        self.procs.append(Storage({
            "proc": proc,
            "on_success": on_success,
            "on_success_args": on_success_args if on_success_args else [],
            "on_success_kwargs": on_success_args if on_success_kwargs else {},
            "on_error": on_error,
            "on_error_args": on_error_args if on_error_args else [],
            "on_error_kwargs": on_error_args if on_error_kwargs else {},
        }))

    def terminate_procs(self):
        for data in self.procs:
            data.proc.terminate()
            for i in range(self.stop_tmo):
                data.proc.poll()
                if data.proc.returncode is not None:
                    break
                time.sleep(1)

    def janitor_procs(self):
        done = []
        for idx, data in enumerate(self.procs):
            data.proc.poll()
            if data.proc.returncode is not None:
                done.append(idx)
                if data.proc.returncode == 0 and data.on_success:
                    getattr(self, data.on_success)(*data.on_success_args, **data.on_success_kwargs)
                elif data.proc.returncode != 0 and data.on_error:
                    getattr(self, data.on_error)(*data.on_error_args, **data.on_error_kwargs)
        for idx in done:
            del self.procs[idx]

    def join_threads(self):
        for thr in self.threads:
            thr.join()

    def janitor_threads(self):
        done = []
        for idx, thr in enumerate(self.threads):
            thr.join(0)
            if not thr.is_alive():
                done.append(idx)
        for idx in sorted(done, reverse=True):
            del self.threads[idx]
        if len(self.threads) > 2:
            self.log.info("threads queue length %d", len(self.threads))

    @lazy
    def config(self):
        try:
            config = ConfigParser.RawConfigParser()
            with codecs.open(rcEnv.paths.nodeconf, "r", "utf8") as filep:
                if sys.version_info[0] >= 3:
                    config.read_file(filep)
                else:
                    config.readfp(filep)
        except Exception as exc:
            self.log.info("error loading config: %", str(exc))
            raise ex.excAbortAction()
        return config

    def reload_config(self):
        unset_lazy(self, "config")

    def get_services_nodenames(self):
        global SERVICES
        global SERVICES_LOCK

        nodenames = set()
        with SERVICES_LOCK:
            for svc in SERVICES.values():
                nodenames |= svc.nodes | svc.drpnodes
        return nodenames

    def set_service_monitor(self, svcname, status=None):
        global MON_DATA
        global MON_DATA_LOCK
        with MON_DATA_LOCK:
            if svcname not in MON_DATA:
                MON_DATA[svcname] = Storage({})
            if status:
                self.log.info(
                    "service %s monitor status change: %s => %s",
                    svcname,
                    MON_DATA[svcname].status if MON_DATA[svcname].status else "none",
                    status
                )
                MON_DATA[svcname].status = status
            MON_DATA[svcname].updated = datetime.datetime.utcnow()

    def get_service_monitor(self, svcname, datestr=False):
        global MON_DATA
        global MON_DATA_LOCK
        with MON_DATA_LOCK:
            if svcname not in MON_DATA:
                self.set_service_monitor(svcname, "idle")
            data = Storage(MON_DATA[svcname])
            if datestr:
                data.updated = data.updated.strftime(DATEFMT)
            return data


#
class Crypt(object):
    """
    A class implement AES encrypt, decrypt and message padding.
    Used by child classes to authenticate senders on data receive.
    """
    @staticmethod
    def _encrypt(message, key, iv):
        return _encrypt(message, key, iv)

    @staticmethod
    def _decrypt(ciphertext, key, iv):
        return _decrypt(ciphertext, key, iv)

    @staticmethod
    def gen_iv(urandom=[], locker=threading.RLock()):
        """
        This is 4x faster than calling os.urandom(16) and prevents
        the "too many files open" issue with concurrent access to os.urandom()
        """
        try:
            return urandom.pop()
        except IndexError:
            try:
                locker.acquire()
                ur = os.urandom(16 * 1024)
                urandom += [ur[i:i + 16] for i in range(16, 1024 * 16, 16)]
                return ur[0:16]
            finally:
                locker.release()

    def decrypt(self, message):
        if hasattr(self, "node"):
            config = self.node.config
        else:
            config = self.config
        message = bdecode(message).rstrip("\0\x00")
        try:
            message = json.loads(message)
        except ValueError:
            self.log.error("misformatted encrypted message: %s", repr(message))
            return None, None
        iv = base64.urlsafe_b64decode(str(message["iv"]))
        data = base64.urlsafe_b64decode(str(message["data"]))
        if message["nodename"] == rcEnv.nodename:
            uuid_key = "uuid"
        else:
            uuid_key = "uuid@"+message["nodename"]
        if not config.has_option("node", uuid_key):
            self.log.error("no %s to use as AES key", uuid_key)
            return None, None
        key = config.get("node", uuid_key).encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        data = bdecode(self._decrypt(data, key, iv))
        try:
            return message["nodename"], json.loads(data)
        except ValueError as exc:
            return message["nodename"], data

    def encrypt(self, data):
        if hasattr(self, "node"):
            config = self.node.config
        else:
            config = self.config
        if not config.has_option("node", "uuid"):
            self.log.error("no uuid to use as AES key")
            return
        key = config.get("node", "uuid").encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        iv = self.gen_iv()
        message = {
            "nodename": rcEnv.nodename,
            "iv": bdecode(base64.urlsafe_b64encode(iv)),
            "data": bdecode(base64.urlsafe_b64encode(self._encrypt(json.dumps(data), key, iv))),
        }
        return (json.dumps(message)+'\0').encode()

    def get_listener_info(self, nodename):
        if hasattr(self, "node"):
            config = self.node.config
        else:
            config = self.config
        if nodename == rcEnv.nodename:
            if not config.has_section("listener"):
                return "127.0.0.1", rcEnv.listener_port
            if config.has_option("listener", "addr@"+nodename):
                addr = config.get("listener", "addr@"+nodename)
            elif config.has_option("listener", "addr"):
                addr = config.get("listener", "addr")
            else:
                addr = "127.0.0.1"
            if config.has_option("listener", "port@"+nodename):
                port = config.getint("listener", "port@"+nodename)
            elif config.has_option("listener", "port"):
                port = config.getint("listener", "port")
            else:
                port = rcEnv.listener_port
        else:
            if not config.has_section("listener"):
                return nodename, rcEnv.listener_port
            if config.has_option("listener", "addr@"+nodename):
                addr = config.get("listener", "addr@"+nodename)
            else:
                addr = nodename
            if config.has_option("listener", "port@"+nodename):
                port = config.getint("listener", "port@"+nodename)
            elif config.has_option("listener", "port"):
                port = config.getint("listener", "port")
            else:
                port = rcEnv.listener_port
        return addr, port

    def daemon_send(self, data, nodename=None, with_result=True):
        if nodename is None:
            nodename = rcEnv.nodename
        addr, port = self.get_listener_info(nodename)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(6.2)
            sock.connect((addr, port))
            message = self.encrypt(data)
            if message is None:
                return
            sock.sendall(message)
            if with_result:
                chunks = []
                while True:
                    chunk = sock.recv(4096)
                    if chunk:
                        chunks.append(chunk)
                    if not chunk or chunk.endswith(b"\x00"):
                        break
                if sys.version_info[0] >= 3:
                    data = b"".join(chunks)
                else:
                    data = "".join(chunks)
                nodename, data = self.decrypt(data)
                return data
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return
        finally:
            sock.close()

#
class Hb(OsvcThread):
    global CLUSTER_DATA
    global CLUSTER_DATA_LOCK

    def __init__(self, name, role=None):
        OsvcThread.__init__(self)
        self.name = name
        self.id = name + "." + role
        self.log = logging.getLogger(rcEnv.nodename+".osvcd."+self.id)
        self.peers = {}

    def status(self):
        data = OsvcThread.status(self)
        data.peers = {}
        for nodename in self.get_services_nodenames():
            if nodename == rcEnv.nodename:
                data.peers[nodename] = {}
                continue
            if "*" in self.peers:
                _data = self.peers["*"]
            else:
                _data = self.peers.get(nodename, Storage({
                        "last": 0,
                        "beating": False,
                    }))
            data.peers[nodename] = {
                "last": datetime.datetime.utcfromtimestamp(_data.last).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "beating": _data.beating,
            }
        return data

    def set_last(self, nodename="*"):
        if nodename not in self.peers:
            self.peers[nodename] = Storage({
                "last": 0,
                "beating": False,
            })
        self.peers[nodename].last = time.time()
        if not self.peers[nodename].beating:
            self.log.info("node %s hb status stale => beating", nodename)
        self.peers[nodename].beating = True

    def is_beating(self, nodename="*"):
        return self.peers.get(nodename, {"beating": False})["beating"]

    def set_beating(self, nodename="*"):
        now = time.time()
        if nodename not in self.peers:
            self.peers[nodename] = Storage({
                "last": 0,
                "beating": False,
            })
        if now > self.peers[nodename].last + self.timeout:
            beating = False
        else:
            beating = True
        if self.peers[nodename].beating != beating:
            if beating:
                self.log.info("node %s hb status stale => beating", nodename)
            else:
                self.log.info("node %s hb status beating => stale", nodename)
        self.peers[nodename].beating = beating

    @staticmethod
    def get_ip_address(ifname):
        ifname = str(ifname)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])

    def get_message(self):
        global HB_MSG
        global HB_MSG_LOCK
        with HB_MSG_LOCK:
            if not HB_MSG:
                # no data to send yet
                return
            return HB_MSG


#
class HbUcast(Hb, Crypt):
    """
    A class factorizing common methods and properties for the unicast
    heartbeat sender and listener child classes.
    """
    DEFAULT_UCAST_PORT = 10000
    DEFAULT_UCAST_TIMEOUT = 15

    def status(self):
        data = Hb.status(self)
        data.stats = self.stats
        data.timeout = self.timeout
        data.config = {
            "addr": self.peer_config[rcEnv.nodename].addr,
            "port": self.peer_config[rcEnv.nodename].port,
        }
        return data

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        self.peer_config = {}
        for option in self.config.options(self.name):
            if "@" in option:
                base_option, nodename = option.split("@", 1)
            else:
                base_option = option
                nodename = rcEnv.nodename
            if nodename not in self.peer_config:
                self.peer_config[nodename] = Storage()
            if base_option == "port":
                self.peer_config[nodename][base_option] = self.config.getint(self.name, option)
            else:
                self.peer_config[nodename][base_option] = self.config.get(self.name, option)
            if base_option == "addr":
                addrinfo = socket.getaddrinfo(self.peer_config[nodename].addr, None)[0]
                self.peer_config[nodename].addr = addrinfo[4][0]

        # timeout
        if self.config.has_option(self.name, "timeout@"+rcEnv.nodename):
            self.timeout = self.config.getint(self.name, "timeout@"+rcEnv.nodename)
        elif self.config.has_option(self.name, "timeout"):
            self.timeout = self.config.getint(self.name, "timeout")
        else:
            self.timeout = self.DEFAULT_UCAST_TIMEOUT

        for nodename in self.peer_config:
            if nodename == rcEnv.nodename:
                continue
            if self.peer_config[nodename].port is None:
                self.peer_config[nodename].port = self.peer_config[rcEnv.nodename].port
        #print(json.dumps(self.peer_config, indent=4))

class HbUcastSender(HbUcast):
    """
    The multicast heartbeat sender class.
    """
    def __init__(self, name):
        HbUcast.__init__(self, name, role="sender")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)
            time.sleep(DEFAULT_HB_PERIOD)

    def status(self):
        data = HbUcast.status(self)
        data["config"] = {}
        return data

    def do(self):
        #self.log.info("sending to %s:%s", self.addr, self.port)
        message = self.get_message()
        if message is None:
            return

        for nodename, config in self.peer_config.items():
            if nodename == rcEnv.nodename:
                continue
            self._do(message, nodename, config)

    def _do(self, message, nodename, config):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(6.2)
            sock.bind((self.peer_config[rcEnv.nodename].addr, 0))
            sock.connect((config.addr, config.port))
            sock.sendall(message)
            self.set_last(nodename)
            self.stats.beats += 1
            self.stats.bytes += len(message)
        except socket.timeout as exc:
            self.stats.errors += 1
            self.log.warning("send timeout")
            self.set_beating(nodename)
        except socket.error as exc:
            self.log.error("send to %s (%s:%d) error: %s", nodename, config.addr, config.port, str(exc))
            return
        finally:
            sock.close()

#
class HbUcastListener(HbUcast):
    """
    The multicast heartbeat listener class.
    """
    def __init__(self, name):
        HbUcast.__init__(self, name, role="listener")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.peer_config[rcEnv.nodename].addr, self.peer_config[rcEnv.nodename].port))
            self.sock.listen(5)
            self.sock.settimeout(0.5)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        self.log.info("listening on %s:%s", self.peer_config[rcEnv.nodename].addr, self.peer_config[rcEnv.nodename].port)

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def do(self):
        self.janitor_threads()

        try:
            conn, addr = self.sock.accept()
        except socket.timeout:
            return
        thr = threading.Thread(target=self.handle_client, args=(conn, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, conn, addr):
        try:
            self._handle_client(conn, addr)
            self.stats.beats += 1
        finally:
            conn.close()

    def _handle_client(self, conn, addr):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK

        chunks = []
        buff_size = 4096
        while True:
            chunk = conn.recv(buff_size)
            self.stats.bytes += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if sys.version_info[0] >= 3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        del chunks

        nodename, data = self.decrypt(data)
        if nodename is None or nodename == rcEnv.nodename:
            # ignore hb data we sent ourself
            return
        if data is None:
            self.stats.errors += 1
            return
        #self.log.info("received data from %s %s", nodename, addr)
        self.set_beating(nodename)
        with CLUSTER_DATA_LOCK:
            CLUSTER_DATA[nodename] = data
        self.set_last(nodename)


#
class HbMcast(Hb, Crypt):
    """
    A class factorizing common methods and properties for the multicast
    heartbeat sender and listener child classes.
    """
    DEFAULT_MCAST_PORT = 10000
    DEFAULT_MCAST_ADDR = "224.3.29.71"
    DEFAULT_MCAST_TIMEOUT = 15

    def status(self):
        data = Hb.status(self)
        data.stats = self.stats
        data.config = {
            "addr": self.addr,
            "port": self.port,
            "intf": self.intf,
            "src_addr": self.src_addr,
            "timeout": self.timeout,
        }
        return data

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        try:
            self.port = self.config.getint(self.name, "port")
        except:
            self.port = self.DEFAULT_MCAST_PORT
        try:
            self.addr = self.config.get(self.name, "addr")
        except:
            self.addr = self.DEFAULT_MCAST_ADDR
        try:
            self.timeout = self.config.getint(self.name, "timeout")
        except:
            self.timeout = self.DEFAULT_MCAST_TIMEOUT
        group = socket.inet_aton(self.addr)
        try:
            self.intf = self.config.get(self.name, "intf")
            self.src_addr = self.get_ip_address(self.intf)
            self.mreq = group + socket.inet_aton(self.src_addr)
        except:
            self.intf = "any"
            self.src_addr = "0.0.0.0"
            self.mreq = struct.pack("4sl", group, socket.INADDR_ANY)

        try:
            self.intf = self.config.get(self.name, "intf")
            self.src_addr = self.get_ip_address(self.intf)
            self.src_naddr = socket.inet_aton(self.src_addr)
        except:
            self.intf = "any"
            self.src_addr = "0.0.0.0"
            self.src_naddr = socket.INADDR_ANY


class HbMcastSender(HbMcast):
    """
    The multicast heartbeat sender class.
    """
    def __init__(self, name):
        HbMcast.__init__(self, name, role="sender")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.group = (self.addr, self.port)
            ttl = struct.pack('b', 32)
            self.sock.settimeout(0.2)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        while True:
            self.do()
            if self.stopped():
                self.sock.close()
                sys.exit(0)
            time.sleep(DEFAULT_HB_PERIOD)

    def do(self):
        #self.log.info("sending to %s:%s", self.addr, self.port)
        message = self.get_message()
        if message is None:
            return

        try:
            sent = self.sock.sendto(message, self.group)
            self.set_last()
            self.stats.beats += 1
            self.stats.bytes += len(message)
        except socket.timeout as exc:
            self.stats.errors += 1
            self.log.warning("send timeout")
            self.set_beating()
        except socket.error as exc:
            self.stats.errors += 1
            self.log.warning("send error: %s" % str(exc))


#
class HbMcastListener(HbMcast):
    """
    The multicast heartbeat listener class.
    """
    def __init__(self, name):
        HbMcast.__init__(self, name, role="listener")

    def run(self):
        try:
            self.configure()
        except ex.excAbortAction:
            return
        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('', self.port))
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
            self.sock.settimeout(0.2)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        self.log.info("listening on %s:%s", self.addr, self.port)

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def do(self):
        self.janitor_threads()

        try:
            data, addr = self.sock.recvfrom(MAX_MSG_SIZE)
            self.stats.beats += 1
            self.stats.bytes += len(data)
        except socket.timeout:
            return
        thr = threading.Thread(target=self.handle_client, args=(data, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, message, addr):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        nodename, data = self.decrypt(message)
        if nodename is None or nodename == rcEnv.nodename:
            # ignore hb data we sent ourself
            return
        if data is None:
            self.stats.errors += 1
            return
        #self.log.info("received data from %s %s", nodename, addr)
        self.set_beating(nodename)
        with CLUSTER_DATA_LOCK:
            CLUSTER_DATA[nodename] = data
        self.set_last(nodename)


#
class Listener(OsvcThread, Crypt):
    sock_tmo = 1.0

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener")
        try:
            self.config = ConfigParser.RawConfigParser()
            self.config.read(rcEnv.paths.nodeconf)
        except:
            self.port = rcEnv.listener_port
            self.addr = "0.0.0.0"
        else:
            try:
                self.port = self.config.getint("listener", "port")
            except:
                self.port = rcEnv.listener_port
            try:
                self.addr = self.config.get("listener", "addr")
            except:
                self.addr = "0.0.0.0"

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.addr, self.port))
            self.sock.listen(5)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.addr, self.port, str(exc))
            return

        self.log.info("listening on %s:%s", self.addr, self.port)

        self.stats = Storage({
            "sessions": Storage({
                "accepted": 0,
                "auth_validated": 0,
                "tx": 0,
                "rx": 0,
                "clients": Storage({
                })
            }),
        })

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def status(self):
        data = OsvcThread.status(self)
        data["stats"] = self.stats
        data["config"] = {
            "port": self.port,
            "addr": self.addr,
        }
        return data

    def do(self):
        done = []
        self.janitor_threads()

        try:
            conn, addr = self.sock.accept()
            self.stats.sessions.accepted += 1
            if addr[0] not in self.stats.sessions.clients:
                self.stats.sessions.clients[addr[0]] = Storage({
                    "accepted": 0,
                    "auth_validated": 0,
                    "tx": 0,
                    "rx": 0,
                })
            self.stats.sessions.clients[addr[0]].accepted += 1
            #self.log.info("accept %s", str(addr))
        except socket.timeout:
            return
        thr = threading.Thread(target=self.handle_client, args=(conn, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, conn, addr):
        try:
            self._handle_client(conn, addr)
        finally:
            conn.close()

    def _handle_client(self, conn, addr):
        chunks = []
        buff_size = 4096
        while True:
            chunk = conn.recv(buff_size)
            self.stats.sessions.rx += len(chunk)
            self.stats.sessions.clients[addr[0]].rx += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if sys.version_info[0] >= 3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        del chunks

        nodename, data = self.decrypt(data)
        #self.log.info("received %s from %s", str(data), nodename)
        self.stats.sessions.auth_validated += 1
        self.stats.sessions.clients[addr[0]].auth_validated += 1
        if data is None:
            cmd = [rcEnv.paths.nodemgr, 'dequeue_actions']
            p = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
            p.communicate()
        else:
            result = self.router(nodename, data)
            if result:
                message = self.encrypt(result)
                conn.sendall(message)
                self.log.info("responded to %s, %s:%d", nodename, addr[0], addr[1])

    #
    # Actions
    #
    def router(self, nodename, data):
        if not isinstance(data, dict):
            return
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        fname = "action_"+data["action"]
        if not hasattr(self, fname):
            return {"error": "action not supported", "status": 1}
        options = data.get("options", {})
        return getattr(self, fname)(nodename, **options)

    def action_daemon_status(self, nodename, **kwargs):
        global THREADS
        global THREADS_LOCK
        data = {}
        with THREADS_LOCK:
            for thr_id, thread in THREADS.items():
                data[thr_id] = thread.status()
        return data

    def action_daemon_stop(self, nodename, **kwargs):
        global THREADS
        global THREADS_LOCK
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            self.log.info("stop daemon requested")
            setattr(sys, "stop_osvcd", 1)
            return {"status": 0}
        with THREADS_LOCK:
            has_thr = thr_id in THREADS
        if not has_thr:
            self.log.info("stop thread requested on non-existing thread")
            return {"error": "thread does not exist"*50, "status": 1}
        self.log.info("stop thread requested")
        with THREADS_LOCK:
            THREADS[thr_id].stop()
        return {"status": 0}

    def action_daemon_start(self, nodename, **kwargs):
        global THREADS
        global THREADS_LOCK
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            return {"error": "no thread specified", "status": 1}
        with THREADS_LOCK:
            has_thr = thr_id in THREADS
        if not has_thr:
            self.log.info("start thread requested on non-existing thread")
            return {"error": "thread does not exist"*50, "status": 1}
        self.log.info("start thread requested")
        with THREADS_LOCK:
            THREADS[thr_id].unstop()
        return {"status": 0}

    def action_get_service_config(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if not svcname:
            return {"error": "no svcname specified", "status": 1}
        fpath = os.path.join(rcEnv.paths.pathetc, svcname+".conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 1}
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve service %s config to %s", svcname, nodename)
        return {"status": 0, "data": buff}

    def action_clear(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if not svcname:
            return {"error": "no svcname specified", "status": 1}
        self.set_service_monitor(svcname, "idle")
        return {"status": 0}


#
class Scheduler(OsvcThread):
    max_runs = 2
    interval = 60

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.scheduler")
        self.log.info("scheduler started")
        self.last_run = time.time()

        while True:
            self.do()
            if self.stopped():
                self.terminate_procs()
                sys.exit(0)

    def do(self):
        self.janitor_procs()
        now = time.time()
        if now < self.last_run + self.interval:
            time.sleep(1)
            return

        if len(self.procs) > self.max_runs:
            self.log.warning("%d scheduler runs are already in progress. skip this run.", self.max_runs)
            return

        self.last_run = now
        self.run_scheduler()

    def run_scheduler(self):
        self.log.info("run schedulers")
        cmd = [rcEnv.paths.nodemgr, 'schedulers']
        try:
            proc = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
        except KeyboardInterrupt:
            return
        self.push_proc(proc=proc)

#
class Monitor(OsvcThread, Crypt):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
    monitor_period = 5

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.last_run = 0
        self.log.info("monitor started")

        while True:
            self.do()
            if self.stopped():
                self.join_threads()
                self.terminate_procs()
                sys.exit(0)

    def do(self):
        self.janitor_threads()
        self.janitor_procs()

        now = time.time()
        if now < self.last_run + self.monitor_period:
            time.sleep(1)
            return

        self.reload_config()
        self.last_run = now
        self.orchestrator()
        self.sync_services_conf()
        self.update_hb_data()

    def sync_services_conf(self):
        global SERVICES
        global SERVICES_LOCK
        confs = self.get_services_configs()
        for svcname, data in confs.items():
            with SERVICES_LOCK:
                if svcname not in SERVICES:
                    continue
            if rcEnv.nodename not in data:
                # need to check if we should have this config ?
                continue
            ref_conf = data[rcEnv.nodename]
            ref_nodename = rcEnv.nodename
            for nodename, conf in data.items():
                if rcEnv.nodename == nodename:
                    continue
                if conf.cksum != ref_conf.cksum and conf.updated > ref_conf.updated:
                    ref_conf = conf
                    ref_nodename = nodename
            if ref_nodename != rcEnv.nodename:
                with SERVICES_LOCK:
                    if rcEnv.nodename in SERVICES[svcname].nodes and \
                       ref_nodename in SERVICES[svcname].drpnodes:
                           # don't fetch drp config from prd nodes
                           return
                self.log.info("node %s has the most recent service %s config", ref_nodename, svcname)
                self.fetch_service_config(svcname, ref_nodename)

    def fetch_service_config(self, svcname, nodename):
        global SERVICES
        global SERVICES_LOCK
        request = {
            "action": "get_service_config",
            "options": {
                "svcname": svcname,
            },
        }
        resp = self.daemon_send(request, nodename=nodename)
        if resp.get("status", 1) != 0:
            self.log.error("unable to fetch service %s config from node %s: received %s", svcname, nodename, resp)
            return
        import tempfile
        with tempfile.NamedTemporaryFile(dir=rcEnv.paths.pathtmp, delete=False) as filep:
            tmpfpath = filep.name
        with codecs.open(tmpfpath, "w", "utf-8") as filep:
            filep.write(resp["data"])
        try:
            with SERVICES_LOCK:
                svc = SERVICES[svcname]
            results = svc._validate_config(path=filep.name)
            if results["errors"] == 0:
                import shutil
                shutil.copy(filep.name, svc.paths.cf)
            else:
                self.log.error("the service %s config fetched from node %s is not valid", svcname, nodename)
        finally:
            os.unlink(tmpfpath)
        self.log.info("the service %s config fetched from node %s is now installed", svcname, nodename)

    def service_command(self, svcname, cmd):
        cmd = [rcEnv.paths.svcmgr, '-s', svcname] + cmd
        self.log.info("execute: %s", " ".join(cmd))
        proc = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
        return proc

    def service_start(self, svcname):
        proc = self.service_command(svcname, ["start"])
        self.push_proc(
            proc=proc,
            on_success="service_start_on_success", on_success_args=[svcname],
            on_error="service_start_on_error", on_error_args=[svcname],
        )

    def service_start_on_error(self, svcname):
        self.set_service_monitor(svcname, "start failed")

    def service_start_on_success(self, svcname):
        self.set_service_monitor(svcname, "idle")

    def orchestrator(self):
        global SERVICES
        global SERVICES_LOCK
        with SERVICES_LOCK:
            svcs = SERVICES.values()
        for svc in svcs:
            self.service_orchestrator(svc)

    def service_orchestrator(self, svc):
        if svc.frozen():
            #self.log.info("service %s orchestrator out (frozen)", svc.svcname)
            return
        if svc.disabled:
            #self.log.info("service %s orchestrator out (disabled)", svc.svcname)
            return
        smon = self.get_service_monitor(svc.svcname)
        if smon.status not in ("ready", "idle"):
            #self.log.info("service %s orchestrator out (mon status %s)", svc.svcname, smon.status)
            return
        status = self.get_service_status(svc.svcname)
        if status in (None, "undef", "n/a"):
            #self.log.info("service %s orchestrator out (agg avail status %s)", svc.svcname, status)
            return
        if svc.anti_affinity:
            intersection = set(self.get_local_svcnames()) & set(svc.anti_affinity)
            if len(intersection) > 0:
                #self.log.info("service %s orchestrator out (anti-affinity with %s)", svc.svcname, ','.join(intersection))
                return

        now = datetime.datetime.utcnow()
        instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
        if svc.clustertype == "failover":
            if smon.status == "ready":
                if instance.avail is "up":
                    self.log.info("abort 'ready' because the local instance has started")
                    self.set_service_monitor(svc.svcname, "idle")
                elif status == "up":
                    self.log.info("abort 'ready' because an instance has started")
                    self.set_service_monitor(svc.svcname, "idle")
                else:
                    if smon.updated < (now - MON_WAIT_READY):
                        self.log.info("failover service %s status %s/ready for %s", svc.svcname, status, now-smon.updated)
                        self.set_service_monitor(svc.svcname, "starting")
                        self.service_start(svc.svcname)
            elif smon.status == "idle":
                if status in ("down", "stdby down", "stdby up"):
                    self.log.info("failover service %s status %s", svc.svcname, status)
                    self.set_service_monitor(svc.svcname, "ready")
        elif svc.clustertype == "flex":
            n_up = self.count_up_service_instances(svc.svcname)
            if smon.status == "ready":
                if (n_up - 1) >= svc.flex_min_nodes:
                    self.log.info("flex service %s instance count reached required minimum while we were ready", svc.svcname, status)
                    self.set_service_monitor(svc.svcname, "idle")
                    return
                if smon.updated < (now - MON_WAIT_READY):
                    self.log.info("flex service %s status %s/ready for %s", svc.svcname, status, now-smon.updated)
                    self.set_service_monitor(svc.svcname, "starting")
                    self.service_start(svc.svcname)
            elif smon.status == "idle":
                if n_up >= svc.flex_min_nodes:
                    return
                if instance.avail not in ("down", "stdby down", "stdby up"):
                    return
                self.log.info("flex service %s started, starting or ready to start instances: %d/%d. local status %s", svc.svcname, n_up, svc.flex_min_nodes, instance.avail)
                self.set_service_monitor(svc.svcname, "ready")

    def count_up_service_instances(self, svcname):
        n_up = 0
        for instance in self.get_service_instances(svcname):
            if instance["avail"] == "up":
                n_up += 1
            elif instance["monitor"]["status"] in ("starting", "ready"):
                n_up += 1
        return n_up

    def get_service(self, svcname):
        global SERVICES
        global SERVICES_LOCK
        with SERVICES_LOCK:
            if svcname not in SERVICES:
                return
        return SERVICES[svcname]

    def get_service_status(self, svcname):
        svc = self.get_service(svcname)
        if svc is None:
            return "unknown"
        if svc.clustertype == "failover":
            return self.get_service_status_failover(svc)
        elif svc.clustertype == "flex":
            return self.get_service_status_flex(svc)
        else:
            return "unknown"
        
    def get_service_status_failover(self, svc):
        astatus = 'undef'
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svc.svcname):
            astatus_l.append(instance["avail"])
            n_instances +=1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_instances == 0:
            astatus = 'n/a'
        elif astatus_s == set(['n/a']):
            astatus = 'n/a'
        elif 'warn' in astatus_l:
            astatus = 'warn'
        elif n_up > 1:
            astatus = 'warn'
        elif n_up == 1:
            astatus = 'up'
        else:
            astatus = 'down'
        return astatus

    def get_service_status_flex(self, svc):
        astatus = 'undef'
        astatus_l = []
        n_instances = 0
        for instance in self.get_service_instances(svc.svcname):
            astatus_l.append(instance["avail"])
            n_instances +=1
        astatus_s = set(astatus_l)

        n_up = astatus_l.count("up")
        if n_instances == 0:
            astatus = 'n/a'
        elif astatus_s == set(['n/a']):
            astatus = 'n/a'
        elif 'warn' in astatus_l:
            astatus = 'warn'
        elif n_up > svc.flex_max_nodes:
            astatus = 'warn'
        elif n_up < svc.flex_min_nodes:
            astatus = 'warn'
        elif n_up == 0:
            astatus = 'down'
        else:
            astatus = 'up'
        return astatus

    def get_local_svcnames(self):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        svcnames = []
        try:
            with CLUSTER_DATA_LOCK:
                for svcname in CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                    if CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["avail"] == "up":
                        svcnames.append(svcname)
        except KeyError:
            return []
        return svcnames

    def get_services_configs(self):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        data = {}
        with CLUSTER_DATA_LOCK:
            for nodename in CLUSTER_DATA:
                try:
                    for svcname in CLUSTER_DATA[nodename]["services"]["config"]:
                        if svcname not in data:
                            data[svcname] = {}
                        data[svcname][nodename] = Storage(CLUSTER_DATA[nodename]["services"]["config"][svcname])
                except KeyError:
                    pass
        return data

    def get_service_instance(self, svcname, nodename):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        try:
            with CLUSTER_DATA_LOCK:
                return Storage(CLUSTER_DATA[nodename]["services"]["status"][svcname])
        except KeyError:
            return

    def get_service_instances(self, svcname):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        instances = []
        try:
            with CLUSTER_DATA_LOCK:
                for node in CLUSTER_DATA:
                    if svcname in CLUSTER_DATA[node]["services"]["status"]:
                        instances.append(CLUSTER_DATA[node]["services"]["status"][svcname])
        except KeyError:
            return []
        return instances

    @staticmethod
    def fsum(fpath):
        with codecs.open(fpath, "r", "utf-8") as filep:
             buff = filep.read()
        cksum = hashlib.md5(buff.encode("utf-8"))
        return cksum.hexdigest()

    def get_last_svc_config(self, svcname):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        with CLUSTER_DATA_LOCK:
            try:
                return CLUSTER_DATA[rcEnv.nodename]["services"]["config"][svcname]
            except KeyError:
                return

    def get_services_config(self):
        global SERVICES
        global SERVICES_LOCK
        from svcBuilder import build
        config = {}
        for cfg in glob.glob(os.path.join(rcEnv.paths.pathetc, "*.conf")):
            svcname = os.path.basename(cfg[:-5])
            linkp = os.path.join(rcEnv.paths.pathetc, svcname)
            if not os.path.exists(linkp):
                continue
            try:
                mtime = os.path.getmtime(cfg)
            except Exception as exc:
                self.log.warning("failed to get %s mtime: %s", cfg, str(exc))
                mtime = 0
            mtime = datetime.datetime.utcfromtimestamp(mtime)
            last_config = self.get_last_svc_config(svcname)
            if last_config is None or mtime > datetime.datetime.strptime(last_config["updated"], DATEFMT):
                self.log.info("compute service %s config checksum", svcname)
                cksum = self.fsum(cfg)
                try:
                    with SERVICES_LOCK:
                        SERVICES[svcname] = build(svcname, minimal=True)
                except Exception as exc:
                    self.log.error("%s build error: %s", svcname, str(exc))
            else:
                cksum = last_config["cksum"]
            config[svcname] = {
                "updated": mtime.strftime(DATEFMT),
                "cksum": self.fsum(cfg),
            }
        return config

    def get_last_svc_status_mtime(self, svcname):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        with CLUSTER_DATA_LOCK:
            try:
                return CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["mtime"]
            except KeyError:
                return 0

    def service_status_fallback(self, svcname):
        self.log.info("slow path service status eval: %s", svcname)
        cmd = [rcEnv.paths.svcmgr, "-s", svcname, "json", "status"]
        try:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
            out, err = proc.communicate()
        except KeyboardInterrupt:
            return {}
        return json.loads(bdecode(out))

    def get_services_status(self, svcnames):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        with CLUSTER_DATA_LOCK:
            try:
                data = CLUSTER_DATA[rcEnv.nodename]["services"]["status"]
            except KeyError:
                data = {}
        for svcname in svcnames:
            fpath = os.path.join(rcEnv.paths.pathvar, svcname, "status.json")
            try:
                mtime = os.path.getmtime(fpath)
            except Exception as exc:
                # force service status refresh
                mtime = time.time() + 1
            last_mtime = self.get_last_svc_status_mtime(svcname)
            if svcname not in data or mtime > last_mtime and svcname in data:
                self.log.info("service %s status changed, update hb payload", svcname)
                try:
                    with open(fpath, 'r') as filep:
                        try:
                            data[svcname] = json.load(filep)
                        except ValueError:
                            data[svcname] = self.service_status_fallback(svcname)
                except Exception:
                     data[svcname] = self.service_status_fallback(svcname)
            data[svcname]["monitor"] = self.get_service_monitor(svcname, datestr=True)

        # purge deleted services
        for svcname in set(data.keys()) - set(svcnames):
            del data[svcname]

        return data

    def update_hb_data(self):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        global HB_MSG
        global HB_MSG_LOCK

        #self.log.info("update heartbeat data to send")
        load_avg = os.getloadavg()
        config = self.get_services_config()
        status = self.get_services_status(config.keys())

        try:
            with CLUSTER_DATA_LOCK:
                CLUSTER_DATA[rcEnv.nodename] = {
                    "updated": datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "services": {
                        "config": config,
                        "status": status,
                    },
                    "load": {
                        "1m": load_avg[0],
                        "5m": load_avg[1],
                        "15m": load_avg[2],
                    },
                }
            with HB_MSG_LOCK:
                HB_MSG = self.encrypt(CLUSTER_DATA[rcEnv.nodename])
        except ValueError:
            self.log.error("failed to refresh local cluster data: invalid json")

    def status(self):
        global CLUSTER_DATA
        global CLUSTER_DATA_LOCK
        self.update_hb_data()
        data = OsvcThread.status(self)
        with CLUSTER_DATA_LOCK:
            data.nodes = dict(CLUSTER_DATA)
        data["services"] = {}
        for svcname in data.nodes[rcEnv.nodename]["services"]["config"]:
            if svcname not in data["services"]:
                data["services"][svcname] = Storage()
            data["services"][svcname].avail = self.get_service_status(svcname)
        return data

class Daemon(object):
    def __init__(self):
        self.handlers = None
        self.threads = {}
        self.config = ConfigParser.RawConfigParser()
        self.last_config_mtime = None
        rcLogger.initLogger(rcEnv.nodename, self.handlers)
        rcLogger.set_namelen(force=30)
        self.log = logging.getLogger(rcEnv.nodename+".osvcd")

    def stop(self):
        self.log.info("daemon stop")
        self.stop_threads()

    def run(self, daemon=True):
        if daemon:
            self.handlers = ["file", "syslog"]
            self._run_daemon()
        else:
            self._run()

    @forked
    def _run_daemon(self):
        self.log.info("daemon started")
        self._run()

    def _run(self):
        while True:
            self.loop()
            if hasattr(sys, "stop_osvcd"):
                self.stop_threads()
                break
        self.log.info("daemon graceful stop")

    def stop_threads(self):
        self.log.info("signal stop to all threads")
        for thr in self.threads.values():
            thr.stop()
        for thr_id, thr in self.threads.items():
            self.log.info("waiting for %s to stop", thr_id)
            thr.join()

    def need_start(self, thr_id):
        if thr_id not in self.threads:
            return True
        thr = self.threads[thr_id]
        if thr.stopped():
            return False
        if thr.is_alive():
            return False
        return True

    def start_threads(self):
        global THREADS
        global THREADS_LOCK

        # a thread can only be started once, allocate a new one if not alive.
        changed = False
        if self.need_start("listener"):
            self.threads["listener"] = Listener()
            self.threads["listener"].start()
            changed = True
        if self.need_start("scheduler"):
            self.threads["scheduler"] = Scheduler()
            self.threads["scheduler"].start()
            changed = True

        self.read_config()

        for name in self.get_config_hb("multicast"):
            hb_id = name + ".listener"
            if self.need_start(hb_id):
                self.threads[hb_id] = HbMcastListener(name)
                self.threads[hb_id].start()
                changed = True
            hb_id = name + ".sender"
            if self.need_start(hb_id):
                self.threads[hb_id] = HbMcastSender(name)
                self.threads[hb_id].start()
                changed = True

        for name in self.get_config_hb("unicast"):
            hb_id = name + ".listener"
            if self.need_start(hb_id):
                self.threads[hb_id] = HbUcastListener(name)
                self.threads[hb_id].start()
                changed = True
            hb_id = name + ".sender"
            if self.need_start(hb_id):
                self.threads[hb_id] = HbUcastSender(name)
                self.threads[hb_id].start()
                changed = True


        if self.need_start("monitor"):
            self.threads["monitor"] = Monitor()
            self.threads["monitor"].start()
            changed = True

        if changed:
            with THREADS_LOCK:
                THREADS = self.threads

    def read_config(self):
        if not os.path.exists(rcEnv.paths.nodeconf):
            return
        try:
            mtime = os.path.getmtime(rcEnv.paths.nodeconf)
        except Exception as exc:
            self.log.warning("failed to get node config mtime: %s", str(exc))
            return
        if self.last_config_mtime is not None and self.last_config_mtime >= mtime:
            return
        try:
            self.config.read(rcEnv.paths.nodeconf)
            if self.last_config_mtime:
                self.log.info("node config reloaded (changed)")
            else:
                self.log.info("node config loaded")
            self.last_config_mtime = mtime
        except Exception as exc:
            self.log.warning("failed to load config: %s", str(exc))

    def get_config_hb(self, hb_type=None):
        hbs = []
        for section in self.config.sections():
            if not section.startswith("hb#"):
                continue
            try:
                section_type = self.config.get(section, "type")
            except:
                section_type = None
            if hb_type and section_type != hb_type:
                continue
            hbs.append(section)
        return hbs

    def loop(self):
        self.start_threads()
        time.sleep(1)

def optparse():
    parser = OptionParser()
    parser.add_option("-f", "--foreground", action="store_false", default=True, dest="daemon")
    return parser.parse_args()

def main():
    options, args = optparse()
    try:
       daemon = Daemon()
       daemon.run(daemon=options.daemon)
    except (KeyboardInterrupt, ex.excSignal):
       daemon.log.info("interrupted")
       daemon.stop()
    except Exception as exc:
       daemon.log.exception(exc)
       daemon.stop()

if __name__ == "__main__":
    main()
