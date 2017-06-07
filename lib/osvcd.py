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
from rcUtilities import justcall, bdecode
from rcStatus import Status
from svcBuilder import build
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

    for fd in range(0, 3):
        try:
            os.close(fd)
        except OSError:
            sys.stderr.write("error closing file: (%d) %s\n" % (e.errno, e.strerror))
            pass

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
    def __init__(self):
        super(OsvcThread, self).__init__()
        self._stop_event = threading.Event()
        self.created = time.time()

    def stop(self):
        self._stop_event.set()

    def unstop(self):
        self._stop_event.clear()

    def stopped(self):
        return self._stop_event.is_set()

    def status(self):
        if self.stopped():
            if self.is_alive():
                state = "STOPPING"
            else:
                state = "STOPPED"
        else:
            if self.is_alive():
                state = "RUNNING"
            else:
                state = "TERMINATED"
        data = Storage({
                "state": state,
                "created": datetime.datetime.utcfromtimestamp(self.created).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
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
        message = bdecode(message).rstrip("\0")
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
        if not self.config.has_option("node", uuid_key):
            self.log.error("no %s to use as AES key", uuid_key)
            return None, None
        key = self.config.get("node", uuid_key).encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        data = bdecode(self._decrypt(data, key, iv))
        try:
            return message["nodename"], json.loads(data)
        except ValueError as exc:
            return message["nodename"], data

    def encrypt(self, data):
        if not self.config.has_option("node", "uuid"):
            self.log.error("no uuid to use as AES key")
            return
        key = self.config.get("node", "uuid").encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        iv = self.gen_iv()
        message = {
            "nodename": rcEnv.nodename,
            "iv": bdecode(base64.urlsafe_b64encode(iv)),
            "data": bdecode(base64.urlsafe_b64encode(self._encrypt(json.dumps(data), key, iv))),
        }
        return (json.dumps(message)+'\0').encode()

#
class Hb(OsvcThread):
    global CLUSTER_DATA

    def __init__(self, name, role=None):
        OsvcThread.__init__(self)
        self.last = 0
        self.name = name
        self.id = name + "." + role
        self.log = logging.getLogger(rcEnv.nodename+".osvcd."+self.id)
        self.beating = False

    def status(self):
        data = OsvcThread.status(self)
        data.last = datetime.datetime.utcfromtimestamp(self.last).strftime('%Y-%m-%dT%H:%M:%SZ')
        data.beating = self.beating
        data.config = {
            "addr": self.addr,
            "port": self.port,
            "intf": self.intf,
            "src_addr": self.src_addr,
            "timeout": self.timeout,
        }
        return data

    def set_last(self):
        self.last = time.time()
        if not self.beating:
            self.log.info("mark heartbeat as up")
        self.beating = True

    def set_beating(self):
        now = time.time()
        if now > self.last + self.timeout:
            beating = False
        else:
            beating = True
        if self.beating != beating:
            self.log.info("mark heartbeat as %s", "up" if beating else "down")
        self.beating = beating

    @staticmethod
    def get_ip_address(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])


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
        data.last = datetime.datetime.utcfromtimestamp(self.last).strftime('%Y-%m-%dT%H:%M:%SZ')
        data.beating = self.beating
        data.stats = self.stats
        data.config = {
            "addr": self.addr,
            "port": self.port,
            "intf": self.intf,
            "src_addr": self.src_addr,
            "timeout": self.timeout,
        }
        return data

    def load_config(self):
        try:
            self.config = ConfigParser.RawConfigParser()
            with codecs.open(rcEnv.paths.nodeconf, "r", "utf8") as filep:
                if sys.version_info[0] >= 3:
                    self.config.read_file(filep)
                else:
                    self.config.readfp(filep)
        except Exception as exc:
            self.log.info("error loading config: %", str(exc))
            raise ex.excAbortAction()

    def configure(self):
        self.stats = Storage({
            "beats": 0,
            "bytes": 0,
            "errors": 0,
        })
        self.load_config()
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

    def format_message(self):
        with CLUSTER_DATA_LOCK:
            if rcEnv.nodename not in CLUSTER_DATA:
                # no data to send yet
                return
            return CLUSTER_DATA[rcEnv.nodename]

    def do(self):
        self.log.info("sending to %s:%s", self.addr, self.port)
        message = self.format_message()
        if message is None:
            return
        message = self.encrypt(self.format_message())
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
        self.threads = []

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
            group = socket.inet_aton(self.addr)
            mreq = struct.pack("4sl", group, self.src_naddr)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.sock.settimeout(0.2)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
            return

        self.log.info("listening on %s:%s", self.addr, self.port)

        while True:
            self.do()
            if self.stopped():
                self.sock.close()
                sys.exit(0)

    def do(self):
        for thr in self.threads:
            thr.join(0)
        try:
            data, addr = self.sock.recvfrom(MAX_MSG_SIZE)
            self.stats.beats += 1
            self.stats.bytes += len(data)
        except socket.timeout:
            self.set_beating()
            return
        thr = threading.Thread(target=self.handle_client, args=(data, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, message, addr):
        nodename, data = self.decrypt(message)
        if nodename is None or nodename == rcEnv.nodename:
            # ignore hb data we sent ourself
            return
        if data is None:
            self.stats.errors += 1
            return
        self.log.info("received data from %s %s", nodename, addr)
        global CLUSTER_DATA
        with CLUSTER_DATA_LOCK:
            CLUSTER_DATA[nodename] = data
        self.set_last()


#
class Listener(OsvcThread, Crypt):
    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener")
        self.threads = []
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
            self.sock.bind((self.addr, self.port))
            self.sock.listen(5)
            self.sock.settimeout(0.2)
        except socket.error as exc:
            self.log.error("init error: %s", str(exc))
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
        for thr in self.threads:
            thr.join(0)
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
            self.log.info("accept %s", str(addr))
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
        self.log.info("received %s from %s", str(data), nodename)
        self.stats.sessions.auth_validated += 1
        self.stats.sessions.clients[addr[0]].auth_validated += 1
        if data is None:
            cmd = [rcEnv.paths.nodemgr, 'dequeue_actions']
            p = Popen(cmd, stdout=None, stderr=None, stdin=None)
            p.communicate()
        else:
            result = self.router(data)
            if result:
                message = self.encrypt(result)
                conn.sendall(message)
                self.log.info("responded to %s, %s:%d", nodename, addr[0], addr[1])

    def router(self, data):
        if not isinstance(data, dict):
            return
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        if not hasattr(self, data["action"]):
            return {"error": "action not supported", "status": 1}
        options = data.get("options", {})
        return getattr(self, data["action"])(**options)

    def daemon_status(self):
        data = {}
        with THREADS_LOCK:
            for thr_id, thread in THREADS.items():
                data[thr_id] = thread.status()
        return data

    def daemon_stop(self, **kwargs):
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

    def daemon_start(self, **kwargs):
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


#
class Scheduler(OsvcThread):
    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.scheduler")
        self.threads = []
        self.log.info("scheduler started")
        self.last_run = time.time()

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)

    def do(self):
        for thr in self.threads:
            thr.join(0)

        now = time.time()
        if now < self.last_run + 60:
            time.sleep(1)
            return

        self.last_run = now
        thr = threading.Thread(target=self.run_scheduler)
        thr.start()
        self.threads.append(thr)

    def run_scheduler(self):
        self.log.info("run schedulers")
        cmd = [rcEnv.paths.nodemgr, 'schedulers']
        p = Popen(cmd, stdout=None, stderr=None, stdin=None)
        p.communicate()

#
class Monitor(OsvcThread):
    """
    The monitoring thread collecting local service states and taking decisions.
    """
    monitor_period = 5

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.monitor")
        self.threads = []
        self.last_run = 0
        self.log.info("monitor started")
        self.data = {}
        self.data_lock = threading.RLock()
        self.services = {}

        while True:
            self.do()
            if self.stopped():
                sys.exit(0)

    def do(self):
        for thr in self.threads:
            thr.join(0)

        now = time.time()
        if now < self.last_run + self.monitor_period:
            time.sleep(1)
            return

        self.last_run = now
        self.orchestrator()
        self.reset_hb_data()

    def service_command(self, svcname, cmd):
        thr = threading.Thread(target=self._service_command, args=(svcname, cmd))
        thr.start()
        self.threads.append(thr)

    def _service_command(self, svcname, cmd):
        cmd = [rcEnv.paths.svcmgr, '-s', svcname] + cmd
        self.log.info("execute: %s", " ".join(cmd))
        p = Popen(cmd, stdout=None, stderr=None, stdin=None)
        p.communicate()
        if p.returncode != 0:
            self.set_service_monitor(svcname, "start failed")
        else:
            self.set_service_monitor(svcname, "idle")

    def orchestrator(self):
        for svc in self.services.values():
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
        if svc.clustertype == "failover":
            if status in ("down", "stdby down"):
                if smon.status == "ready":
                    if smon.updated < (now - MON_WAIT_READY):
                        self.log.info("failover service %s status %s/ready for %s", svc.svcname, status, now-smon.updated)
                        self.set_service_monitor(svc.svcname, "starting")
                        self.service_command(svc.svcname, ["start"])
                else:
                    self.log.info("failover service %s status %s", svc.svcname, status)
                    self.set_service_monitor(svc.svcname, "ready")
        elif svc.clustertype == "flex":
            n_up = self.count_up_service_instances(svc.svcname)
            if n_up < svc.flex_min_nodes:
                instance = self.get_service_instance(svc.svcname, rcEnv.nodename)
                if smon.status == "ready":
                    if smon.updated < (now - MON_WAIT_READY):
                        self.log.info("flex service %s status %s/ready for %s", svc.svcname, status, now-smon.updated)
                        self.set_service_monitor(svc.svcname, "starting")
                        self.service_command(svc.svcname, ["start"])
                elif instance.avail in ("down", "stdby down"):
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

    def get_service_status(self, svcname):
        status = Status()
        for instance in self.get_service_instances(svcname):
            status += Status(instance["avail"])
        return str(status)

    def get_local_svcnames(self):
        svcnames = []
        try:
            with CLUSTER_DATA_LOCK:
                for svcname in CLUSTER_DATA[rcEnv.nodename]["services"]["status"]:
                    if CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["avail"] == "up":
                        svcnames.append(svcname)
        except KeyError:
            return []
        return svcnames

    def get_service_instance(self, svcname, nodename):
        try:
            with CLUSTER_DATA_LOCK:
                return Storage(CLUSTER_DATA[nodename]["services"]["status"][svcname])
        except KeyError:
            return

    def get_service_instances(self, svcname):
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
        with CLUSTER_DATA_LOCK:
            try:
                return CLUSTER_DATA[rcEnv.nodename]["services"]["config"][svcname]
            except KeyError:
                return

    def get_services_config(self):
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
                    self.services[svcname] = build(svcname, minimal=True)
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
        with CLUSTER_DATA_LOCK:
            try:
                return CLUSTER_DATA[rcEnv.nodename]["services"]["status"][svcname]["mtime"]
            except KeyError:
                return 0

    def service_status_fallback(self, svcname):
        self.log.info("slow path service status eval: %s", svcname)
        cmd = [rcEnv.paths.svcmgr, "-s", svcname, "json", "status"]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        return json.loads(bdecode(out))

    def get_services_status(self, svcnames):
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
            if mtime <= last_mtime and svcname in data:
                continue
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

    def set_service_monitor(self, svcname, status=None):
        with self.data_lock:
            if svcname not in self.data:
                self.data[svcname] = Storage({})
            if status:
                self.log.info(
                    "service %s monitor status change: %s => %s",
                    svcname,
                    self.data[svcname].status if self.data[svcname].status else "none",
                    status
                )
                self.data[svcname].status = status
            self.data[svcname].updated = datetime.datetime.utcnow()

    def get_service_monitor(self, svcname, datestr=False):
        with self.data_lock:
            if svcname not in self.data:
                self.set_service_monitor(svcname, "idle")
            data = Storage(self.data[svcname])
            if datestr:
                data.updated = data.updated.strftime(DATEFMT)
            return data

    def reset_hb_data(self):
        #self.log.info("update heartbeat data to send")
        load_avg = os.getloadavg()
        config = self.get_services_config()
        status = self.get_services_status(config.keys())

        global CLUSTER_DATA
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
        except ValueError:
            self.log.error("failed to refresh local cluster data: invalid json")

    def status(self):
        data = OsvcThread.status(self)
        with CLUSTER_DATA_LOCK:
            data.nodes = dict(CLUSTER_DATA)
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

    def stop_threads(self):
        for thr in self.threads.values():
            thr.stop()
        for thr in self.threads.values():
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

        if self.need_start("monitor"):
            self.threads["monitor"] = Monitor()
            self.threads["monitor"].start()
            changed = True

        global THREADS
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
