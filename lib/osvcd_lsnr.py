"""
Listener Thread
"""
import json
import os
import sys
import socket
import logging
import threading
import codecs
import time
import select
import shutil
import traceback
import uuid
from subprocess import Popen, PIPE

try:
    import ssl
    has_ssl = True
except Exception:
    has_ssl = False

import six
import osvcd_shared as shared
import rcExceptions as ex
from six.moves import queue
from rcGlobalEnv import rcEnv
from storage import Storage
from rcUtilities import bdecode, drop_option, chunker, svc_pathcf, \
                        split_svcpath, fmt_svcpath, is_service, factory, \
                        makedirs, mimport, set_lazy, lazy
from converters import convert_size, print_duration

RELAY_DATA = {}
RELAY_LOCK = threading.RLock()
RELAY_SLOT_MAX_AGE = 24 * 60 * 60
RELAY_JANITOR_INTERVAL = 10 * 60
JANITORS_INTERVAL = 0.5

class DontClose(Exception):
    pass

class Listener(shared.OsvcThread):
    sock_tmo = 1.0
    events_grace_period = True
    sockmap = {}
    crl_expire = 0
    crl_mode = None
    last_janitors = 0

    @lazy
    def certfs(self):
        mod = mimport("res", "fs")
        res = mod.Mount(rid="fs#certs", mount_point=rcEnv.paths.certs, device="shmfs", fs_type="tmpfs", mount_options="size=1m")
        set_lazy(res, "log",  self.log)
        return res

    @lazy
    def ca(self):
        secpath = shared.NODE.oget("cluster", "ca")
        if secpath is None:
            secpath = "system/sec/ca-" + self.cluster_name
        secname, namespace, kind = split_svcpath(secpath)
        return factory("sec")(secname, namespace=namespace, volatile=True)

    @lazy
    def cert(self):
        secpath = shared.NODE.oget("cluster", "cert")
        if secpath is None:
            secpath = "system/sec/cert-" + self.cluster_name
        secname, namespace, kind = split_svcpath(secpath)
        return factory("sec")(secname, namespace=namespace, volatile=True)

    def prepare_certs(self):
        makedirs(rcEnv.paths.certs)
        if rcEnv.sysname == "Linux":
            self.certfs.start()
        if not self.ca.exists():
            raise ex.excInitError("secret %s does not exist" % self.ca.svcpath)
        data = self.ca.decode_key("certificate_chain")
        if data is None:
            raise ex.excInitError("secret key %s.%s is not set" % (self.ca.svcpath, "certificate_chain"))
        ca_cert_chain = os.path.join(rcEnv.paths.certs, "ca_certificate_chain")
        self.log.info("write %s", ca_cert_chain)
        with open(ca_cert_chain, "w") as fo:
            fo.write(data)
        crl_path = self.fetch_crl()
        data = self.cert.decode_key("certificate_chain")
        if data is None:
            raise ex.excInitError("secret key %s.%s is not set" % (self.cert.svcpath, "certificate_chain"))
        cert_chain = os.path.join(rcEnv.paths.certs, "certificate_chain")
        self.log.info("write %s", cert_chain)
        with open(cert_chain, "w") as fo:
            fo.write(data)
        data = self.cert.decode_key("private_key")
        if data is None:
            raise ex.excInitError("secret key %s.%s is not set" % (self.cert.svcpath, "private_key"))
        private_key = os.path.join(rcEnv.paths.certs, "private_key")
        self.log.info("write %s", private_key)
        with open(private_key, "w") as fo:
            fo.write(data)
        return ca_cert_chain, cert_chain, private_key, crl_path

    def fetch_crl(self):
        crl = shared.NODE.oget("listener", "crl")
        if not crl:
            return
        if crl == rcEnv.paths.crl:
            self.crl_mode = "internal"
            try:
                buff = self.ca.decode_key("crl")
            except Exception as exc:
                buff = None
            if buff is None:
                self.log.info("cluster ca crl configured but empty")
                return
            else:
                self.log.info("write %s", crl)
                with open(crl, "w") as fo:
                    fo.write(buff)
                return crl
        self.crl_mode = "external"
        if os.path.exists(crl):
            return crl
        crl_path = os.path.join(rcEnv.paths.certs, "certificate_revocation_list")
        try:
            shared.NODE.urlretrieve(crl, crl_path)
            # TODO: extract expire from crl
            self.crl_expire = time.time() + 60*60*24
            return crl_path
        except Exception as exc:
            self.log.warning("crl fetch failed: %s", exc)
            return

    def setup_socktls(self):
        if not has_ssl:
            self.log.info("skip tls listener init: ssl module import error")
            return
        try:
            self.tls_port = self.config.getint("listener", "tls_port")
        except Exception:
            self.tls_port = rcEnv.listener_tls_port
        try:
            self.tls_addr = self.config.get("listener", "tls_addr")
        except Exception:
            self.tls_addr = "0.0.0.0"
        try:
            self.tls_sock.close()
        except Exception:
            pass
        try:
            ca_cert_chain, cert_chain, private_key, crl = self.prepare_certs()
            context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_cert_chain(cert_chain, keyfile=private_key)
            context.load_verify_locations(ca_cert_chain)
            if crl:
                self.log.info("tls crl %s", crl)
                context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
                context.load_verify_locations(crl)
            self.log.info("tls stats: %s", context.cert_store_stats())
            addrinfo = socket.getaddrinfo(self.tls_addr, None)[0]
            self.tls_addr = addrinfo[4][0]
            self.tls_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            self.tls_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tls_sock.bind((self.tls_addr, self.tls_port))
            self.tls_sock.listen(128)
            self.tls_sock.settimeout(self.sock_tmo)
            self.tls_wrapped_sock = context.wrap_socket(self.tls_sock)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.tls_addr, self.tls_port, exc)
            return
        except ex.excInitError as exc:
            self.log.info("skip tls listener init: %s", exc)
            return
        except Exception as exc:
            self.log.info("failed tls listener init: %s", exc)
            return
        self.log.info("listening on %s:%s using tls and client auth", self.tls_addr, self.tls_port)
        self.sockmap[self.tls_wrapped_sock.fileno()] = self.tls_wrapped_sock

    def setup_sock(self):
        try:
            self.port = self.config.getint("listener", "port")
        except Exception:
            self.port = rcEnv.listener_port
        try:
            self.addr = self.config.get("listener", "addr")
        except Exception:
            self.addr = "0.0.0.0"

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.addr, self.port))
            self.sock.listen(128)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.addr, self.port, exc)
            return
        self.log.info("listening on %s:%s using aes encryption", self.addr, self.port)
        self.sockmap[self.sock.fileno()] = self.sock

    def setup_sockux(self):
        if os.name == "nt":
            return
        if not os.path.exists(rcEnv.paths.lsnruxsockd):
            os.makedirs(rcEnv.paths.lsnruxsockd)
        try:
            if os.path.isdir(rcEnv.paths.lsnruxsock):
                shutil.rmtree(rcEnv.paths.lsnruxsock)
            else:
                os.unlink(rcEnv.paths.lsnruxsock)
        except Exception:
            pass
        try:
            self.sockux = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockux.bind(rcEnv.paths.lsnruxsock)
            self.sockux.listen(1)
            self.sockux.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s error: %s", rcEnv.paths.lsnruxsock, exc)
            return
        self.log.info("listening on %s", rcEnv.paths.lsnruxsock)
        self.sockmap[self.sockux.fileno()] = self.sockux

    def setup_socks(self):
        for sock in self.sockmap.values():
            try:
                sock.close()
            except socket.error:
                pass
        self.sockmap = {}
        self.setup_socktls()
        self.setup_sock()
        self.setup_sockux()

    def run(self):
        shared.NODE.listener = self
        self.set_tid()
        self.last_relay_janitor = 0
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener")
        self.events_clients = []
        self.stats = Storage({
            "sessions": Storage({
                "accepted": 0,
                "auth_validated": 0,
                "tx": 0,
                "rx": 0,
                "alive": Storage({}),
                "clients": Storage({})
            }),
        })

        self.setup_socks()

        while True:
            try:
                self.do()
            except socket.error as exc:
                self.log.warning(exc)
                self.setup_socks()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                for sock in self.sockmap.values():
                    sock.close()
                self.join_threads()
                if rcEnv.sysname == "Linux":
                    self.certfs.stop()
                sys.exit(0)

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "port": self.port,
            "addr": self.addr,
        }
        return data

    def reconfigure(self):
        shared.NODE.listener = self
        self.setup_socks()

    def do(self):
        self.reload_config()
        ts = time.time()
        if ts > self.last_janitors + JANITORS_INTERVAL:
            self.janitor_crl()
            self.janitor_procs()
            self.janitor_threads()
            self.janitor_events()
            self.janitor_relay()
        self.last_janitors = ts

        fds = select.select([fno for fno in self.sockmap], [], [], self.sock_tmo)
        if self.sock_tmo and fds == ([], [], []):
            return
        for fd in fds[0]:
            sock = self.sockmap[fd]
            cn = None
            try:
                conn = None
                conn, addr = sock.accept()
                self.stats.sessions.accepted += 1
                if len(addr) == 0:
                    addr = ["local"]
                    encrypted = False
                elif isinstance(sock, ssl.SSLSocket):
                    encrypted = False
                    cert = conn.getpeercert()
                    subject = dict(x[0] for x in cert['subject'])
                    cn = subject["commonName"]
                    usr = factory("usr")(cn, namespace="system", volatile=True, log=self.log)
                    if not usr.exists():
                        self.log.warning("refused user connection: %s (valid cert, unknown user)", cn)
                        conn.close()
                        continue
                else:
                    encrypted = True
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
                continue
            except Exception:
                if conn:
                    conn.close()
                continue
            try:
                thr = threading.Thread(target=self.handle_client, args=(conn, addr, encrypted, cn))
                thr.start()
                self.threads.append(thr)
            except RuntimeError as exc:
                self.log.warning(exc)
                conn.close()

    def janitor_crl(self):
        if not self.tls_sock:
            return
        if not self.crl_mode:
            return
        try:
            mtime = os.path.getmtime(rcEnv.paths.crl)
        except Exception:
            mtime = 0

        if self.crl_mode == "internal":
            if not "crl" in self.ca.data_keys():
                if os.path.exists(rcEnv.paths.crl):
                    try:
                        self.log.info("remove %s", rcEnv.paths.crl)
                        os.unlink(rcEnv.paths.crl)
                    except Exception as exc:
                        self.log.warning("remove %s: %s", rcEnv.paths.crl, exc)
                        return
                else:
                    return
            else:
                try:
                    refmtime = os.path.getmtime(self.ca.paths.cf)
                except Exception:
                    return
                if not mtime or mtime > refmtime:
                    return
                self.log.info("refresh crl: installed version is %s older than %s", print_duration(refmtime-mtime), self.ca.svcpath)
        elif self.crl_mode == "external":
            if not mtime or mtime > self.crl_expire:
                return
            self.log.info("refresh crl: installed version is expired since %s", print_duration(self.crl_expire-mtime))
        try:
            fno = self.tls_wrapped_sock.fileno()
            self.tls_wrapped_sock.close()
            del self.sockmap[fno]
        except Exception as exc:
            print(exc)
            pass
        self.setup_socktls()

    def janitor_relay(self):
        """
        Purge expired relay.
        """
        now = time.time()
        if now - self.last_relay_janitor < RELAY_JANITOR_INTERVAL:
            return
        self.last_relay_janitor = now
        with RELAY_LOCK:
            for key in [k for k in RELAY_DATA]:
                age = now - RELAY_DATA[key]["updated"]
                if age > RELAY_SLOT_MAX_AGE:
                    self.log.info("drop relay slot %s aged %s", key, print_duration(age))
                    del RELAY_DATA[key]

    def janitor_events(self):
        """
        Send queued events to all subscribed clients.

        Don't dequeue messages during the first 2 seconds of the listener lifetime,
        so clients have a chance to reconnect after a daemon restart and loose an
        event.
        """
        if self.events_grace_period:
            if time.time() > self.created + 2:
                self.events_grace_period = False
            else:
                return
        done = []
        while True:
            # recv pings to avoid filling the client send buffer
            for idx, (conn, encrypted, sid) in enumerate(self.events_clients):
                while True:
                    try:
                        buff = conn.recv(4096)
                    except Exception as exc:
                        break
                    if not buff:
                        break
            try:
                event = shared.EVENT_Q.get(False, 0)
            except queue.Empty:
                break
            emsg = self.encrypt(event)
            msg = self.msg_encode(event)
            to_remove = []
            for idx, (conn, encrypted, sid) in enumerate(self.events_clients):
                if encrypted:
                    _msg = emsg
                else:
                    _msg = msg
                try:
                    conn.sendall(_msg)
                except socket.error as exc:
                    to_remove.append(idx)
            for idx in to_remove:
                try:
                    self.events_clients[idx][0].close()
                except Exception:
                    pass
                try:
                    del self.events_clients[idx]
                except KeyError:
                    pass
                try:
                    del self.stats.sessions.alive[sid]
                except KeyError:
                    pass

    def handle_client(self, conn, addr, encrypted, cn):
        try:
            close = True
            sid = str(uuid.uuid4())
            self.stats.sessions.alive[sid] = Storage({
                "created": time.time(),
                "addr": addr[0],
                "encrypted": encrypted,
                "progress": "init",
            })
            self._handle_client(conn, addr, encrypted, cn, sid)
        except DontClose:
            close = False
        finally:
            if close:
                del self.stats.sessions.alive[sid]
                conn.close()

    def _handle_client(self, conn, addr, encrypted, cn, sid):
        chunks = []
        buff_size = 4096
        conn.setblocking(0)
        while True:
            ready = select.select([conn], [], [conn], 6)
            if ready[0]:
                chunk = conn.recv(buff_size)
            else:
                self.log.warning("timeout waiting for data from client %s", addr[0])
                return
            if ready[2]:
                self.log.debug("exceptional condition on socket with client %s", addr[0])
                return
            self.stats.sessions.rx += len(chunk)
            self.stats.sessions.clients[addr[0]].rx += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if six.PY3:
            data = b"".join(chunks)
            dequ = data == b"dequeue_actions"
        else:
            data = "".join(chunks)
            dequ = data == "dequeue_actions"
        del chunks

        if dequ:
            self.stats.sessions.alive[sid].progress = "dequeue_actions"
            p = Popen([rcEnv.paths.nodemgr, 'dequeue_actions'],
                      stdout=None, stderr=None, stdin=None,
                      close_fds=os.name!="nt")
            return

        if encrypted:
            nodename, data = self.decrypt(data, sender_id=addr[0])
        else:
            try:
                data = self.msg_decode(data)
            except ValueError:
                pass
            nodename = rcEnv.nodename
        #self.log.info("received %s from %s", str(data), nodename)
        self.stats.sessions.auth_validated += 1
        self.stats.sessions.clients[addr[0]].auth_validated += 1
        if data is None:
            return
        result = self.router(nodename, data, conn, addr, encrypted, cn, sid)
        if result:
            self.stats.sessions.alive[sid].progress = "sending %s result" % self.stats.sessions.alive[sid].progress
            conn.setblocking(1)
            if encrypted:
                message = self.encrypt(result)
            else:
                message = self.msg_encode(result)
            for chunk in chunker(message, 64*1024):
                try:
                    conn.sendall(chunk)
                except socket.error as exc:
                    if exc.errno == 32:
                        # broken pipe
                        self.log.info(exc)
                    else:
                        self.log.warning(exc)
                    break
            message_len = len(message)
            self.stats.sessions.tx += message_len
            self.stats.sessions.clients[addr[0]].tx += message_len

    def log_request(self, msg, nodename, lvl="info", cn=None, addr=None, **kwargs):
        """
        Append the request origin to the message logged by the router action"
        """
        if not msg:
            return
        if addr[0] == "local" or not cn or not addr:
            origin = "requested by %s" % nodename
        origin = "requested by %s@%s" % (cn, addr[0])
        if lvl == "error":
            fn = self.log.error
        if lvl == "warning":
            fn = self.log.warning
        else:
            fn = self.log.info
        fn("%s %s", msg, origin)

    #########################################################################
    #
    # Actions
    #
    #########################################################################
    def multiplex(self, node, fname, options, data, original_nodename, conn, addr, encrypted, cn, sid):
        try:
            del data["node"]
        except Exception:
            pass
        result = {"nodes": {}, "status": 0}
        svcpath = options.get("svcpath")
        if node == "ANY" and svcpath:
            svcnodes = [n for n in shared.CLUSTER_DATA if shared.CLUSTER_DATA[n].get("services", {}).get("config", {}).get(svcpath)]
            try:
                if rcEnv.nodename in svcnodes:
                    # prefer to not relay, if possible
                    nodenames = [rcEnv.nodename]
                else:
                    nodenames = [svcnodes[0]]
            except KeyError:
                return {"error": "unknown service", "status": 1}
        else:
            nodenames = shared.NODE.nodes_selector(node, data=shared.CLUSTER_DATA)
        for nodename in nodenames:
            if nodename == rcEnv.nodename:
                _result = getattr(self, fname)(nodename, conn=conn, encrypted=encrypted,
                                               addr=addr, sid=sid, cn=cn, **options)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
            else:
                _result = self.daemon_send(data, nodename=nodename, silent=True)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
        return result

    def create_multiplex(self, fname, options, data, original_nodename, conn, addr, encrypted, cn, sid):
        h = {}
        for svcpath, svcdata in options.get("data", {}).items():
            nodes = svcdata.get("DEFAULT", {}).get("nodes")
            placement = svcdata.get("DEFAULT", {}).get("placement", "nodes order")
            if nodes:
                nodes = shared.NODE.nodes_selector(nodes, data=shared.CLUSTER_DATA)
            else:
                nodes = [n for n in shared.CLUSTER_DATA if shared.CLUSTER_DATA[n].get("services", {}).get("config", {}).get(svcpath)]
            if nodes:
                if rcEnv.nodename in nodes:
                    node = rcEnv.nodename
                else:
                    node = nodes[0]
            else:
                node = rcEnv.nodename
            if node not in h:
                h[node] = {}
            h[node][svcpath] = svcdata
        result = {"nodes": {}, "status": 0}
        for nodename, optdata in h.items():
            _options = {}
            _options.update(options)
            _options["data"] = optdata
            if nodename == rcEnv.nodename:
                _result = getattr(self, fname)(nodename, conn=conn, encrypted=encrypted,
                                               addr=addr, sid=sid, cn=cn, **_options)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
            else:
                _data = {}
                _data.update(data)
                _data["options"] = _options
                self.log_request("relay create/update %s to %s" % (",".join([p for p in optdata]), nodename), original_nodename, cn=cn, addr=addr)
                _result = self.daemon_send(_data, nodename=nodename, silent=True)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
        return result

    def router(self, nodename, data, conn, addr, encrypted, cn, sid):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        if not isinstance(data, dict):
            return {"error": "invalid data format", "status": 1}
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        fname = "action_"+data["action"]
        if not hasattr(self, fname):
            return {"error": "action not supported", "status": 1}
        # prepare options, sanitized for use as keywords
        options = {}
        for key, val in data.get("options", {}).items():
            options[str(key)] = val
        self.stats.sessions.alive[sid].progress = fname
        # TODO: rbac
        if fname == "action_create":
            return self.create_multiplex(fname, options, data, nodename, conn, addr, encrypted, cn, sid)
        node = data.get("node")
        if node:
            return self.multiplex(node, fname, options, data, nodename, conn, addr, encrypted, cn, sid)
        return getattr(self, fname)(nodename, conn=conn, encrypted=encrypted,
                                    addr=addr, sid=sid, cn=cn, **options)

    def action_run_done(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        action = kwargs.get("action")
        rids = kwargs.get("rids")
        if not rids is None:
            rids = ",".join(sorted(rids))
        if not action:
            return {"status": 0}
        sig = (action, svcpath, rids)
        with shared.RUN_DONE_LOCK:
            shared.RUN_DONE.add(sig)
        return {"status": 0}

    def action_relay_tx(self, nodename, **kwargs):
        """
        Store a relay heartbeat payload emitted by <nodename>.
        """
        cluster_id = kwargs.get("cluster_id", "")
        cluster_name = kwargs.get("cluster_name", "")
        key = "/".join([cluster_id, nodename])
        with RELAY_LOCK:
            RELAY_DATA[key] = {
                "msg": kwargs.get("msg"),
                "updated": time.time(),
                "cluster_name": cluster_name,
                "cluster_id": cluster_id,
                "ipaddr": kwargs.get("addr", [""])[0],
            }
        return {"status": 0}

    def action_relay_rx(self, nodename, **kwargs):
        """
        Serve to <nodename> the relay heartbeat payload emitted by the node in
        <slot>.
        """
        cluster_id = kwargs.get("cluster_id", "")
        _nodename = kwargs.get("slot")
        key = "/".join([cluster_id, _nodename])
        with RELAY_LOCK:
            if key not in RELAY_DATA:
                return {"status": 1, "error": "no data"}
            return {
                "status": 0,
                "data": RELAY_DATA[key]["msg"],
                "updated": RELAY_DATA[key]["updated"],
            }

    def action_daemon_relay_status(self, nodename, **kwargs):
        data = {}
        with RELAY_LOCK:
            for _nodename, _data in RELAY_DATA.items():
                data[_nodename] = {
                    "cluster_name": _data.get("cluster_name", ""),
                    "updated": _data.get("updated", 0),
                    "ipaddr": _data.get("ipaddr", ""),
                    "size": len(_data.get("msg", "")),
                }
        return data

    def action_daemon_blacklist_clear(self, nodename, **kwargs):
        """
        Clear the senders blacklist.
        """
        self.blacklist_clear()
        return {"status": 0}

    def action_daemon_blacklist_status(self, nodename, **kwargs):
        """
        Return the senders blacklist.
        """
        return {"status": 0, "data": self.get_blacklist()}

    def action_daemon_stats(self, nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        data = {
            "timestamp": time.time(),
            "daemon": shared.DAEMON.stats(),
            "node": {
                "cpu": {
                    "time": shared.NODE.cpu_time(),
                 },
            },
            "services": {},
        }
        with shared.THREADS_LOCK:
            for thr_id, thr in shared.THREADS.items():
                data[thr_id] = thr.thread_stats()
        with shared.SERVICES_LOCK:
            for svc in shared.SERVICES.values():
                _data = svc.pg_stats()
                if _data:
                    data["services"][svc.svcpath] = _data
        return {"status": 0, "data": data}

    def action_nodes_info(self, nodename, **kwargs):
        """
        Return a hash indexed by nodename, containing the info
        required by the node selector algorithm.
        """
        return {"status": 0, "data": self.nodes_info()}

    def action_daemon_status(self, nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        data = {
            "pid": shared.DAEMON.pid,
            "cluster": {
                "name": self.cluster_name,
                "id": self.cluster_id,
                "nodes": self.cluster_nodes,
            }
        }
        with shared.THREADS_LOCK:
            for thr_id, thread in shared.THREADS.items():
                data[thr_id] = thread.status(**kwargs)
        return data

    def wait_shutdown(self):
        def still_shutting():
            for svcpath, smon in shared.SMON_DATA.items():
                if smon.local_expect == "shutdown":
                    return True
            return False
        while still_shutting():
            time.sleep(1)

    def action_daemon_shutdown(self, nodename, **kwargs):
        """
        Care with locks
        """
        self.log_request("shutdown daemon", nodename, **kwargs)
        with shared.THREADS_LOCK:
            shared.THREADS["scheduler"].stop()
            mon = shared.THREADS["monitor"]
        if self.stopped() or shared.NMON_DATA.status == "shutting":
            self.log.info("already shutting")
            # wait for service shutdown to finish before releasing the dup client
            while True:
                if mon._shutdown:
                    break
                time.sleep(0.3)
            return {"status": 0}
        try:
            self.set_nmon("shutting")
            mon.kill_procs()
            for svcpath in shared.SMON_DATA:
                _, _, kind = split_svcpath(svcpath)
                if kind not in ("svc", "vol"):
                    continue
                self.set_smon(svcpath, local_expect="shutdown")
            self.wait_shutdown()

            # send a last status to peers so they can takeover asap
            mon.update_hb_data()

            mon._shutdown = True
            shared.wake_monitor("services shutdown done")
        except Exception as exc:
            self.log.exception(exc)

        self.log.info("services are now shutdown")
        while True:
            with shared.THREADS_LOCK:
                if not shared.THREADS["monitor"].is_alive():
                    break
            time.sleep(0.3)
        shared.DAEMON_STOP.set()
        return {"status": 0}

    def action_daemon_stop(self, nodename, **kwargs):
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            self.log_request("stop daemon", nodename, **kwargs)
            if kwargs.get("upgrade"):
                self.set_nmon(status="upgrade")
                self.log.info("announce upgrade state")
            else:
                self.set_nmon(status="maintenance")
                self.log.info("announce maintenance state")
            time.sleep(5)
            shared.DAEMON_STOP.set()
            return {"status": 0}
        elif thr_id == "tx":
            thr_ids = [thr_id for thr_id in shared.THREADS.keys() if thr_id.endswith("tx")]
        else:
            thr_ids = [thr_id]
        for thr_id in thr_ids:
            with shared.THREADS_LOCK:
                has_thr = thr_id in shared.THREADS
            if not has_thr:
                self.log_request("stop thread requested on non-existing thread", nodename, **kwargs)
                return {"error": "thread does not exist"*50, "status": 1}
            self.log_request("stop thread %s" % thr_id, nodename, **kwargs)
            with shared.THREADS_LOCK:
                shared.THREADS[thr_id].stop()
            if thr_id == "scheduler":
                shared.wake_scheduler()
            elif thr_id == "monitor":
                shared.wake_monitor("shutdown")
            elif thr_id.endswith("tx"):
                shared.wake_heartbeat_tx()
            if kwargs.get("wait", False):
                with shared.THREADS_LOCK:
                    shared.THREADS[thr_id].join()
        return {"status": 0}

    def action_daemon_start(self, nodename, **kwargs):
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            return {"error": "no thread specified", "status": 1}
        with shared.THREADS_LOCK:
            has_thr = thr_id in shared.THREADS
        if not has_thr:
            self.log_request("start thread requested on non-existing thread", nodename, **kwargs)
            return {"error": "thread does not exist"*50, "status": 1}
        self.log_request("start thread %s" % thr_id, nodename, **kwargs)
        with shared.THREADS_LOCK:
            shared.THREADS[thr_id].unstop()
        return {"status": 0}

    def action_get_node_config(self, nodename, **kwargs):
        fmt = kwargs.get("format")
        if fmt == "json":
            return self._action_get_node_config_json(nodename, **kwargs)
        else:
            return self._action_get_node_config_file(nodename, **kwargs)

    def _action_get_node_config_json(self, nodename, **kwargs):
        try:
            return shared.NODE.print_config_data()
        except Exception as exc:
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_node_config_file(self, nodename, **kwargs):
        fpath = os.path.join(rcEnv.paths.pathetc, "node.conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 3}
        mtime = os.path.getmtime(fpath)
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve node config to %s", nodename)
        return {"status": 0, "data": buff, "mtime": mtime}

    def action_get_service_config(self, nodename, **kwargs):
        fmt = kwargs.get("format")
        if fmt == "json":
            return self._action_get_service_config_json(nodename, **kwargs)
        else:
            return self._action_get_service_config_file(nodename, **kwargs)

    def action_get_secret_key(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        key = kwargs.get("key")
        try:
            return {"status": 0, "data": shared.SERVICES[svcpath].decode_key(key)}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_service_config_json(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        evaluate = kwargs.get("evaluate")
        impersonate = kwargs.get("impersonate")
        try:
            return shared.SERVICES[svcpath].print_config_data(evaluate=evaluate, impersonate=impersonate)
        except Exception as exc:
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_service_config_file(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        if not svcpath:
            return {"error": "no svcpath specified", "status": 1}
        if shared.SMON_DATA.get(svcpath, {}).get("status") in ("purging", "deleting") or \
           shared.SMON_DATA.get(svcpath, {}).get("global_expect") in ("purged", "deleted"):
            return {"error": "delete in progress", "status": 2}
        fpath = svc_pathcf(svcpath)
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 3}
        mtime = os.path.getmtime(fpath)
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve service %s config to %s", svcpath, nodename)
        return {"status": 0, "data": buff, "mtime": mtime}

    def action_wake_monitor(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath", "<unspecified>")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        shared.wake_monitor(reason="service %s notification" % svcpath)
        return {"status": 0}

    def action_clear(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        if svcpath is None:
            return {"error": "no svcpath specified", "status": 1}
        smon = self.get_service_monitor(svcpath)
        if smon.status.endswith("ing"):
            return {"info": "skip clear on %s instance" % smon.status, "status": 0}
        self.log_request("service %s clear" % svcpath, nodename, **kwargs)
        self.set_smon(svcpath, status="idle", reset_retries=True)
        return {"status": 0}

    def get_service_slaves(self, svcpath, slaves=None):
        """
        Recursive lookup of service slaves.
        """
        if slaves is None:
            slaves = set()
        _, namespace, _ = split_svcpath(svcpath)

        def set_ns(path, parent_ns):
            name, _namespace, kind = split_svcpath(path)
            if _namespace:
                return path
            else:
                return fmt_svcpath(name, parent_ns, kind)

        for nodename in shared.CLUSTER_DATA:
            try:
                data = shared.CLUSTER_DATA[nodename]["services"]["status"][svcpath]
            except KeyError:
                continue
            slaves.add(svcpath)
            new_slaves = set(data.get("slaves", [])) | set(data.get("scaler_slaves", []))
            new_slaves = set([set_ns(slave, namespace) for slave in new_slaves])
            new_slaves -= slaves
            for slave in new_slaves:
                slaves |= self.get_service_slaves(slave, slaves)
        return slaves

    def action_set_service_monitor(self, nodename, **kwargs):
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        if svcpath is None:
            return {"error": ["no svcpath specified"], "status": 1}
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        reset_retries = kwargs.get("reset_retries", False)
        stonith = kwargs.get("stonith")
        svcpaths = set([svcpath])
        if global_expect != "scaled":
            svcpaths |= self.get_service_slaves(svcpath)
        errors = []
        info = []
        for svcpath in svcpaths:
            try:
                self.validate_global_expect(svcpath, global_expect)
                new_ge = self.validate_destination_node(svcpath, global_expect)
                if new_ge:
                    global_expect = new_ge
            except ex.excAbortAction as exc:
                info.append(str(exc))
            except ex.excError as exc:
                errors.append(str(exc))
            else:
                info.append("service %s target state set to %s" % (svcpath, global_expect))
                self.set_smon(
                    svcpath, status=status,
                    local_expect=local_expect, global_expect=global_expect,
                    reset_retries=reset_retries,
                    stonith=stonith,
                )
        ret = {"status": len(errors)}
        if info:
            ret["info"] = info
        if errors:
            ret["error"] = errors
        return ret

    def validate_destination_node(self, svcpath, global_expect):
        """
        For a placed@<dst> <global_expect> (move action) on <svcpath>,

        Raise an excError if
        * the service <svcpath> does not exist
        * the service <svcpath> topology is failover and more than 1
          destination node was specified
        * the specified destination is not a service candidate node
        * no destination node specified
        * an empty destination node is specified in a list of destination
          nodes

        Raise an excAbortAction if
        * the avail status of the instance on the destination node is up
        """
        if global_expect is None:
            return
        try:
            global_expect, destination_nodes = global_expect.split("@", 1)
        except ValueError:
            return
        if global_expect != "placed":
            return
        instances = self.get_service_instances(svcpath)
        if not instances:
            raise ex.excError("service does not exist")
        if destination_nodes == "<peer>":
            instance = list(instances.values())[0]
            if instance.get("topology") == "flex":
                raise ex.excError("no destination node specified")
            else:
                nodes = [node for node, inst in instances.items() \
                              if inst.get("avail") not in ("up", "warn", "n/a")]
                count = len(nodes)
                if count == 0:
                    raise ex.excError("no candidate destination node")
                svc = self.get_service(svcpath)
                return "placed@%s" % self.placement_ranks(svc, nodes)[0]
        else:
            destination_nodes = destination_nodes.split(",")
            count = len(destination_nodes)
            if count == 0:
                raise ex.excError("no destination node specified")
            instance = list(instances.values())[0]
            if count > 1 and instance.get("topology") == "failover":
                raise ex.excError("only one destination node can be specified for "
                                  "a failover service")
            for destination_node in destination_nodes:
                if not destination_node:
                    raise ex.excError("empty destination node")
                if destination_node not in instances:
                    raise ex.excError("destination node %s has no service instance" % \
                                      destination_node)
                instance = instances[destination_node]
                if instance["avail"] == "up":
                    raise ex.excAbortAction("instance on destination node %s is "
                                            "already up" % destination_node)

    def validate_global_expect(self, svcpath, global_expect):
        if global_expect is None:
            return
        if global_expect in ("frozen", "aborted", "provisioned"):
            # allow provision target state on just-created service
            return

        # wait for service to appear
        for i in range(5):
            instances = self.get_service_instances(svcpath)
            if instances:
                break
            if not is_service(svcpath):
                break
            time.sleep(1)
        if not instances:
            raise ex.excError("service does not exist")

        for nodename, _data in instances.items():
            status = _data.get("monitor", {}).get("status", "unknown")
            if status != "idle" and "failed" not in status and "wait" not in status:
                raise ex.excError("%s instance on node %s in %s state"
                                  "" % (svcpath, nodename, status))

        if global_expect not in ("started", "stopped"):
            return
        agg = Storage(shared.AGG.get(svcpath, {}))
        if global_expect == "started" and agg.avail == "up":
            raise ex.excAbortAction("service %s is already started" % svcpath)
        elif global_expect == "stopped" and agg.avail in ("down", "stdby down", "stdby up"):
            raise ex.excAbortAction("service %s is already stopped" % svcpath)
        if agg.avail in ("n/a", "undef"):
            raise ex.excAbortAction()

    def action_set_node_monitor(self, nodename, **kwargs):
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        self.set_nmon(
            status=status,
            local_expect=local_expect, global_expect=global_expect,
        )
        return {"status": 0}

    def lock_accepted(self, name, lock_id):
        for nodename, node in shared.CLUSTER_DATA.items():
            lock = node.get("locks", {}).get(name)
            if not lock:
                return False
            if lock.get("id") != lock_id:
                return False
        return True

    def lock_acquire(self, nodename, name, timeout=None):
        if timeout is None:
            timeout = 10
        if nodename not in self.cluster_nodes:
            return
        lock_id = None
        deadline = time.time() + timeout
        situation = 0
        while time.time() < deadline:
            if not lock_id:
                lock_id = self._lock_acquire(nodename, name)
                if not lock_id:
                    if situation != 1:
                        self.log.info("claim %s lock refused (already claimed)", name)
                    situation = 1
                    time.sleep(0.5)
                    continue
                self.log.info("claimed %s lock: %s", name, lock_id)
            if shared.LOCKS.get(name, {}).get("id") != lock_id:
                self.log.info("claim %s dropped", name)
                lock_id = None
                continue
            if self.lock_accepted(name, lock_id):
                self.log.info("locked %s", name)
                return lock_id
            time.sleep(0.5)
        self.log.warning("claim timeout on %s lock", name)
        self.lock_release(name, lock_id, silent=True)

    def lock_release(self, name, lock_id, silent=False):
        with shared.LOCKS_LOCK:
            if not lock_id or shared.LOCKS.get(name, {}).get("id") != lock_id:
                return
            del shared.LOCKS[name]
        shared.wake_monitor(reason="unlock", immediate=True)
        if not silent:
            self.log.info("released %s", name)

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

    def action_lock(self, nodename, **kwargs):
        name = kwargs.get("name")
        timeout = kwargs.get("timeout")
        lock_id = self.lock_acquire(nodename, name, timeout)
        if lock_id:
            result = {
                "data": {
                    "id": lock_id,
                },
                "status": 0,
            }
        else:
            result = {"status": 1}
        return result

    def action_unlock(self, nodename, **kwargs):
        name = kwargs.get("name")
        lock_id = kwargs.get("id")
        self.lock_release(name, lock_id)
        result = {"status": 0}
        return result

    def action_leave(self, nodename, **kwargs):
        self.log.info("node %s is leaving", nodename)
        if nodename not in self.cluster_nodes:
            self.log.info("node %s already left", nodename)
            return {"status": 0}
        try:
            self.remove_cluster_node(nodename)
            return {"status": 0}
        except Exception as exc:
            return {
                "status": 1,
                "error": [str(exc)],
            }

    def action_collector_xmlrpc(self, nodename, **kwargs):
        args = kwargs.get("args", [])
        kwargs = kwargs.get("kwargs", {})
        shared.COLLECTOR_XMLRPC_QUEUE.insert(0, (args, kwargs))
        result = {
            "status": 0,
        }
        return result

    def action_join(self, nodename, **kwargs):
        if nodename in self.cluster_nodes:
            new_nodes = self.cluster_nodes
            self.log.info("node %s rejoins", nodename)
        else:
            new_nodes = self.cluster_nodes + [nodename]
            self.add_cluster_node(nodename)
        result = {
            "status": 0,
            "data": {
                "node": {
                    "data": {
                        "node": {},
                        "cluster": {},
                    },
                },
            },
        }
        config = shared.NODE.get_config(cluster=False)
        if config.has_option("node", "env"):
            result["data"]["node"]["data"]["node"]["env"] = shared.NODE.env
        if config.has_option("cluster", "nodes"):
            result["data"]["node"]["data"]["cluster"]["nodes"] = " ".join(new_nodes)
        if config.has_option("cluster", "name"):
            result["data"]["node"]["data"]["cluster"]["name"] = self.cluster_name
        if config.has_option("cluster", "drpnodes"):
            result["data"]["node"]["data"]["cluster"]["drpnodes"] = " ".join(self.cluster_drpnodes)
        if config.has_option("cluster", "id"):
            result["data"]["node"]["data"]["cluster"]["id"] = self.cluster_id
        if config.has_option("cluster", "quorum"):
            result["data"]["node"]["data"]["cluster"]["quorum"] = self.quorum
        if config.has_option("cluster", "dns"):
            result["data"]["node"]["data"]["cluster"]["dns"] = " ".join(shared.NODE.dns)
        for section in config.sections():
            if section.startswith("hb#") or \
               section.startswith("stonith#") or \
               section.startswith("pool#") or \
               section.startswith("network#") or \
               section.startswith("arbitrator#"):
                result["data"]["node"]["data"][section] = {}
                for key, val in config.items(section):
                    result["data"]["node"]["data"][section][key] = val
        from cluster import ClusterSvc
        svc = ClusterSvc(volatile=True, node=shared.NODE)
        if svc.exists():
            result["data"]["cluster"] = {
                "data": svc.print_config_data(),
                "mtime": os.stat(svc.paths.cf).st_mtime,
            }
        return result

    def action_node_action(self, nodename, **kwargs):
        """
        Execute a nodemgr command on behalf of a peer node.
        kwargs:
        * cmd: list
        * sync: boolean
        """
        sync = kwargs.get("sync", True)
        action_mode = kwargs.get("action_mode", True)
        cmd = kwargs.get("cmd")
        if cmd is None or len(cmd) == 0:
            self.log_request("node action ('cmd' not set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }

        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("--daemon", cmd)
        cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "nodemgr.py")] + cmd
        if action_mode and "--local" not in cmd:
            cmd += ["--local"]
        self.log_request("run '%s'" % " ".join(cmd), nodename, **kwargs)
        if sync:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
            result = {
                "status": 0,
                "data": {
                    "out": bdecode(out),
                    "err": bdecode(err),
                    "ret": proc.returncode,
                },
            }
        else:
            proc = Popen(cmd, stdin=None, close_fds=True)
            self.push_proc(proc)
            result = {
                "status": 0,
            }
        return result

    def action_create(self, nodename, **kwargs):
        """
        Execute a svcmgr create action, feeding the services definitions
        passed in <data>.
        """
        data = kwargs.get("data")
        if not data:
            return {"status": 0, "info": "no data"}
        sync = kwargs.get("sync", True)
        namespace = kwargs.get("namespace")
        provision = kwargs.get("provision")
        restore = kwargs.get("restore")
        self.log_request("create/update %s" % ",".join([p for p in data]), nodename, **kwargs)
        cmd = ["create", "--config=-"]
        if namespace:
            cmd.append("--namespace="+namespace)
        if restore:
            cmd.append("--restore")
        proc = self.service_command(None, cmd, stdin=json.dumps(data))
        if sync:
            out, err = proc.communicate()
            result = {
                "status": 0,
                "data": {
                    "out": bdecode(out),
                    "err": bdecode(err),
                    "ret": proc.returncode,
                },
            }
        else:
            self.push_proc(proc)
            result = {
                "status": 0,
            }
        if provision:
            for path in data:
                self.set_smon(path, global_expect="provisioned")
        return result

    def action_service_action(self, nodename, **kwargs):
        """
        Execute a CRM command on behalf of a peer node.
        kwargs:
        * svcpath: str
        * cmd: list
        * sync: boolean
        """
        sync = kwargs.get("sync", True)
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        if svcpath is None:
            self.log_request("service action (no 'svcpath' set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }
        cmd = kwargs.get("cmd")
        if self.get_service(svcpath) is None and "create" not in cmd:
            self.log_request("service action (%s not installed)" % svcpath, nodename, lvl="warning", **kwargs)
            return {
                "err": "service not found",
                "status": 1,
            }
        if cmd is None or len(cmd) == 0:
            self.log_request("service action (no 'cmd' set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }

        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("--daemon", cmd)
        cmd.append("--local")
        cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "svcmgr.py"), "-s", svcpath] + cmd
        self.log_request("run '%s'" % " ".join(cmd), nodename, **kwargs)
        if sync:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
            try:
                result = json.loads(out)
            except Exception:
                result = {
                    "status": 0,
                    "data": {
                        "out": bdecode(out),
                        "err": bdecode(err),
                        "ret": proc.returncode,
                    },
                }
        else:
            proc = Popen(cmd, stdin=None, close_fds=True)
            self.push_proc(proc)
            result = {
                "status": 0,
            }
        return result

    def action_events(self, nodename, **kwargs):
        encrypted = kwargs.get("encrypted")
        conn = kwargs.get("conn")
        sid = kwargs.get("sid")
        self.events_clients.append((conn, encrypted, sid))
        raise DontClose

    def action_service_logs(self, nodename, **kwargs):
        """
        Send service logs.
        kwargs:
        * svcpath
        * conn: the connexion socket to the requester
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        svcpath = kwargs.get("svcpath")
        if not svcpath:
            svcpath = kwargs.get("svcname")
        svc = self.get_service(svcpath)
        if svcpath is None or svc is None:
            return {
                "status": 1,
            }
        logfile = os.path.join(svc.log_d, svc.svcname+".log")
        self._action_logs(nodename, logfile, "service %s" % svcpath, **kwargs)

    def action_node_logs(self, nodename, **kwargs):
        """
        Send node logs.
        kwargs:
        * conn: the connexion socket to the requester
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        logfile = os.path.join(rcEnv.paths.pathlog, "node.log")
        self._action_logs(nodename, logfile, "node", **kwargs)

    def _action_logs(self, nodename, logfile, obj, **kwargs):
        conn = kwargs.get("conn")
        encrypted = kwargs.get("encrypted")
        backlog = kwargs.get("backlog")
        if backlog is None:
            backlog = 1024 * 10
        else:
            backlog = convert_size(backlog, _to='B')
        skip = 0
        if backlog > 0:
            fsize = os.path.getsize(logfile)
            if backlog > fsize:
                skip = 0
            else:
                skip = fsize - backlog

        with open(logfile, "r") as ofile:
            if backlog > 0:
                self.log.debug("send %s log to node %s, backlog %d",
                               obj, nodename, backlog)
                try:
                    ofile.seek(skip)
                except Exception as exc:
                    self.log.info(str(exc))
                    ofile.seek(0)
            elif backlog < 0:
                self.log.info("send %s log to node %s, whole file",
                              obj, nodename)
                ofile.seek(0)
            else:
                self.log.info("follow %s log for node %s",
                              obj, nodename)
                ofile.seek(0, 2)
            lines = []
            msg_size = 0
            conn.settimeout(1)
            loops = 0

            if skip:
                # drop first line (that is incomplete as the seek placed the
                # cursor in the middle
                line = ofile.readline()

            while True:
                if self.stopped():
                    break
                line = ofile.readline()
                line_size = len(line)
                if line_size == 0:
                    if msg_size > 0:
                        if encrypted:
                            message = self.encrypt(lines)
                        else:
                            message = self.msg_encode(lines)
                        try:
                            conn.sendall(message)
                        except Exception as exc:
                            if hasattr(exc, "errno") and getattr(exc, "errno") == 32:
                                # Broken pipe (client has left)
                                break
                    if backlog != 0:
                        # don't follow file
                        break
                    else:
                        loops += 1
                        # follow
                        if loops > 10:
                            try:
                                conn.send(b"\0")
                                loops = 0
                            except Exception as exc:
                                self.log.info("stop following %s log for node %s: %s",
                                              obj, nodename, exc)
                                break
                        time.sleep(0.1)
                        lines = []
                        msg_size = 0
                        continue
                lines.append(line)
                msg_size += line_size
                if msg_size > shared.MAX_MSG_SIZE:
                    if encrypted:
                        message = self.encrypt(lines)
                    else:
                        message = self.msg_encode(lines)
                    conn.sendall(message)
                    msg_size = 0
                    lines = []

    def action_ask_full(self, nodename, **kwargs):
        """
        Reset the gen number of the dataset of a peer node to force him
        to resend a full.
        """
        peer = kwargs.get("peer")
        if peer is None:
            raise ex.excError("The 'peer' option must be set")
        if peer == rcEnv.nodename:
            raise ex.excError("Can't ask a full from ourself")
        if peer not in self.cluster_nodes:
            raise ex.excError("Can't ask a full from %s: not in cluster.nodes" % peer)
        shared.REMOTE_GEN[peer] = 0
        result = {
            "info": "remote %s asked for a full" % peer,
            "status": 0,
        }
        return result

