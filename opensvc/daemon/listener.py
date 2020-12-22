"""
Listener Thread
"""
import base64
import importlib
import json
import os
import pkgutil
import sys
import socket
import logging
import time
import select
import shutil
import traceback
import uuid
import fnmatch
import re
import datetime
from foreign.six.moves.urllib.parse import urlparse, parse_qs # pylint: disable=import-error
from subprocess import Popen
from errno import EADDRINUSE, ECONNRESET, EPIPE

try:
    import ssl
    import foreign.h2 as h2
    from foreign.h2.config import H2Configuration
    from foreign.h2.connection import H2Connection
    from foreign.hyper.common.headers import HTTPHeaderMap
    has_ssl = True
except Exception:
    has_ssl = False

try:
    import foreign.jwt as jwt
    from foreign.jwt.algorithms import RSAAlgorithm
    has_jwt = True
except Exception:
    has_jwt = False

import foreign.six as six
import daemon.shared as shared
import core.exceptions as ex
from foreign.six.moves import queue
from env import Env
from utilities.storage import Storage
from core.comm import Headers
from utilities.chunker import chunker
from utilities.naming import split_path, fmt_path, factory, split_fullname
from utilities.files import makedirs
from utilities.drivers import driver_import
from utilities.lazy import set_lazy, lazy, unset_lazy
from utilities.converters import print_duration
from utilities.string import bencode, bdecode
from utilities.uri import Uri
from utilities.render.listener import fmt_listener

if six.PY2:
    class _ConnectionResetError(Exception):
        pass
    class _ConnectionAbortedError(Exception):
        pass
    ConnectionResetError = _ConnectionResetError
    ConnectionAbortedError = _ConnectionAbortedError

RE_LOG_LINE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-2][0-9]:[0-6][0-9]:[0-6][0-9],[0-9]{3} .* \| ")
JANITORS_INTERVAL = 0.5
ICON = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAABigAAAYoBM5cwWAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAJKSURBVDiNbZJLSNRRFMZ/5/5HbUidRSVSuGhMzUKiB9SihYaJQlRSm3ZBuxY9JDRb1NSi7KGGELRtIfTcJBjlItsohT0hjcpQSsM0CMfXzP9xWszM35mpA/dy7+Wc7/vOd67wn9gcuZ8bisa3xG271LXthTdNL/rZ0B0VQbNzA+mX2ra+kL04d86NxY86QpEI8catv0+SIyOMNnr6aa4ba/aylL2cTdVI6tBwrbfUXvKeOXY87Ng2jm3H91dNnWrd++U89kIx7jw48+DMf0bcOtk0MA5gABq6egs91+pRCKc01lXOnG2tn4yAKUYkmWpATDlqevRjdb4PYMWDrSiVqIKCosMX932vAYoQQ8bCgGoVajcDmIau3jxP9bj6/igoFqiTuCeLkDQQQOSEDm3PMQEnfxeqhYlSH6Si6WF4EJjIZE+1AqiGCAZ3GoT1yYcEuSqqMDBacOXMo5JORDJBRJa9V0qMqkiGfHwt1vORlW3ND9ZdB/mZNDANJNmgUXcsnTmx+WCBvuH8G6/GC276BpLmA95XMxvVQdC5NOYkkC8ocG9odRCRzEkI0yzF3pn+SM2SKrfJiCRQYp9uqf9l/p2E3pIdr20DkCvBS6o64tMvtzLTfmTiQlGh05w1iSFyQ23+R3rcsjsqrlPr4X3Q5f6nOw7/iOwpX+wEsyLNwLcIB6TsSQzASon+1n83unbboTtiaczz3FVXD451VG+cawfyEAHPGcdzruPOHpOKp39SdcvzyAqdOh3GsyoBsLxJ1hS+F4l42Xl/Abn0Ctwc5dldAAAAAElFTkSuQmCC")

ROUTED_ACTIONS = {
    "node": {
        "logs": "node_logs",
        "backlogs": "node_backlogs",
    },
    "object": {
        "logs": "object_logs",
        "backlogs": "object_backlogs",
    },
}


class Close(Exception):
    pass


class DontClose(Exception):
    pass


class Listener(shared.OsvcThread):
    name = "listener"
    stage = "init"
    events_grace_period = True
    sock_tmo = 1.0
    sockmap = {}
    last_janitors = 0
    crl_expire = 0
    crl_mode = None
    sockux = None
    sockuxh2 = None
    sock = None
    tls_sock = None
    tls_context = None
    tls_port = -1
    tls_addr = ""
    port = -1
    addr = ""
    handlers = {}

    @lazy
    def certfs(self):
        mod = driver_import("resource", "fs")
        res = mod.Fs(rid="fs#certs", mount_point=Env.paths.certs, device="tmpfs", fs_type="tmpfs", mount_options="rw,nosuid,nodev,noexec,relatime,size=1m")
        set_lazy(res, "log",  self.log)
        return res

    @lazy
    def ca(self):
        secpaths = shared.NODE.oget("cluster", "ca")
        if not secpaths:
            secpaths = ["system/sec/ca-" + self.cluster_name]
        secs = []
        for secpath in secpaths:
            secname, namespace, kind = split_path(secpath)
            sec = factory("sec")(secname, namespace=namespace, volatile=True, node=shared.NODE)
            if not sec.exists():
                self.log.warning("ca %s does not exist: ignore", secpath)
                continue
            if "certificate_chain" not in sec.data_keys():
                self.log.warning("ca %s has no certificate key: ignore", secpath)
                continue
            secs.append(sec)
        return secs

    @lazy
    def cert(self):
        secpath = shared.NODE.oget("cluster", "cert")
        if secpath is None:
            secpath = "system/sec/cert-" + self.cluster_name
        secname, namespace, kind = split_path(secpath)
        return factory("sec")(secname, namespace=namespace, volatile=True, node=shared.NODE)

    def prepare_certs(self):
        makedirs(Env.paths.certs)
        if Env.sysname == "Linux" and self.ca and self.cert and self.cert.exists():
            self.certfs.start()
            os.chmod(Env.paths.certs, 0o0755)

        # concat ca certificates
        ca_certs = os.path.join(Env.paths.certs, "ca_certificates")
        try:
            open(ca_certs, 'w').close()
        except OSError:
            pass
        for ca in self.ca:
            data = ca.decode_key("certificate_chain")
            if data is None:
                self.log.warning("secret key %s.%s is not set" % (ca.path, "certificate_chain"))
                continue
            self.log.info("add %s certificate chain to %s", ca.path, ca_certs)
            with open(ca_certs, "a") as fo:
                fo.write(bdecode(data))

        # listener cert chain
        data = self.cert.decode_key("certificate_chain")
        if data is None:
            raise ex.InitError("secret key %s.%s is not set" % (self.cert.path, "certificate_chain"))
        cert_chain = os.path.join(Env.paths.certs, "certificate_chain")
        self.log.info("write %s", cert_chain)

        # listener private key
        with open(cert_chain, "w") as fo:
            fo.write(bdecode(data))
        data = self.cert.decode_key("private_key")
        if data is None:
            raise ex.InitError("secret key %s.%s is not set" % (self.cert.path, "private_key"))

        # listener private key
        private_key = os.path.join(Env.paths.certs, "private_key")
        self.log.info("write %s", private_key)
        with open(private_key, "w+") as fo:
            pass
        os.chmod(private_key, 0o0600)
        with open(private_key, "w") as fo:
            fo.write(bdecode(data))

        # revocations
        crl_path = self.fetch_crl()

        return ca_certs, cert_chain, private_key, crl_path

    def fetch_crl(self):
        crl = shared.NODE.oget("listener", "crl")
        if not crl:
            return

        if crl == Env.paths.crl:
            self.crl_mode = "internal"
            try:
                os.unlink(crl)
            except OSError:
                pass
            buff = ""
            for ca in self.ca:
                ca.unset_lazy("cd")
                if "crl" not in ca.data_keys():
                    continue
                try:
                    buff += bdecode(ca.decode_key("crl"))
                except Exception as exc:
                    self.log.error("decode %s crl error: %s", ca.path, exc)
            if buff:
                try:
                    with open(crl, "w") as fo:
                        fo.write(buff)
                    return crl
                except Exception as exc:
                    self.log.error("install %s error: %s", crl, exc)
            return

        self.crl_mode = "external"
        if os.path.exists(crl):
            return crl
        crl_path = os.path.join(Env.paths.certs, "certificate_revocation_list")
        secure = shared.NODE.oget("node", "secure_fetch")
        try:
            with Uri(crl, secure=secure).fetch() as fpath:
                shutil.copy(fpath, crl_path)
            # TODO: extract expire from crl
            self.crl_expire = time.time() + 60*60*24
            return crl_path
        except Exception as exc:
            self.log.warning("crl fetch failed: %s", exc)
            return

    def get_http2_ssl_context(self):
        """
        This function creates an SSLContext object that is suitably configured for
        HTTP/2. If you're working with Python TLS directly, you'll want to do the
        exact same setup as this function does.
        """
        ca_certs, cert_chain, private_key, crl = self.prepare_certs()
        # Get the basic context from the standard library.
        ctx = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        #ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.verify_mode = ssl.CERT_OPTIONAL
        ctx.load_cert_chain(cert_chain, keyfile=private_key)
        ctx.load_verify_locations(ca_certs)
        if crl:
            self.log.info("tls crl %s", crl)
            ctx.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
            ctx.load_verify_locations(crl)
        self.log.info("tls stats: %s", ctx.cert_store_stats())

        # RFC 7540 Section 9.2: Implementations of HTTP/2 MUST use TLS version 1.2
        # or higher. Disable TLS 1.1 and lower.
        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

        # RFC 7540 Section 9.2.1: A deployment of HTTP/2 over TLS 1.2 MUST disable
        # compression.
        ctx.options |= ssl.OP_NO_COMPRESSION

        # RFC 7540 Section 9.2.2: "deployments of HTTP/2 that use TLS 1.2 MUST
        # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256". In practice, the
        # blacklist defined in this section allows only the AES GCM and ChaCha20
        # cipher suites with ephemeral key negotiation.
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")

        # We want to negotiate using NPN and ALPN. ALPN is mandatory, but NPN may
        # be absent, so allow that. This setup allows for negotiation of HTTP/1.1.
        ctx.set_alpn_protocols(["h2", "http/1.1"])

        try:
            ctx.set_npn_protocols(["h2", "http/1.1"])
        except NotImplementedError:
            pass

        return ctx

    def run(self):
        shared.NODE.listener = self
        self.set_tid()
        self.last_relay_janitor = 0
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.listener"), {"node": Env.nodename, "component": self.name})
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

        self.register_handlers()
        self.setup_socks()
        self.stage = "ready"

        while True:
            try:
                self.do()
                self.update_status()
            except socket.error as exc:
                self.log.warning(exc)
                self.setup_socks()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                for sock in self.sockmap.values():
                    sock.close()
                self.join_threads()
                if Env.sysname == "Linux":
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
        unset_lazy(self, "ca")
        unset_lazy(self, "cert")
        unset_lazy(self, "certfs")
        self.setup_socks()

    def register_handlers(self):
        self.register_core_handlers()
        self.register_driver_handlers()

    def register_driver_handlers(self):
        from utilities.drivers import iter_drivers
        from core.objects.svcdict import KEYS, SECTIONS
        for mod in iter_drivers(SECTIONS):
            if not hasattr(mod, "DRIVER_HANDLERS"):
                continue
            for handler_class in mod.DRIVER_HANDLERS:
                handler = handler_class()
                for method, path in handler.routes:
                    path = "drivers/resource/%s/%s/%s" % (mod.DRIVER_GROUP, mod.DRIVER_BASENAME, path)
                    self.handlers[(method, path)] = handler
                    if method:
                        self.log.info("register handler %s /%s", method, path)

    def register_core_handlers(self):
        def onerror(name):
            import traceback
            traceback.print_exc()
        handlers_path = [os.path.join(Env.paths.pathsvc, Env.package, "daemon", "handlers")]
        for modinfo in pkgutil.walk_packages(handlers_path, 'daemon.handlers.', onerror=onerror):
            if hasattr(modinfo, "ispkg"):
                name = modinfo.name
                ispkg = modinfo.ispkg
            else:
                name = modinfo[1]
                ispkg = modinfo[2]
            if ispkg:
                continue
            if name.split(".")[-1] not in ("get", "post"):
                continue
            try:
                mod = importlib.import_module(name)
                handler = mod.Handler()
                if handler.path in self.handlers:
                    continue
                for method, path in handler.routes:
                    self.handlers[(method, path)] = handler
                    if method:
                        self.log.info("register handler %s /%s", method, path)
            except Exception as exc:
                self.alert("error", "error registering handler %s: %s" % (name, exc))
                continue

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
            try:
                conn = None
                conn, addr = sock.accept()
                self.stats.sessions.accepted += 1
                if fd == self.sockux.fileno():
                    tls = False
                    addr = ["local"]
                    scheme = "raw"
                    encrypted = False
                elif fd == self.sockuxh2.fileno():
                    tls = False
                    addr = ["local"]
                    scheme = "h2"
                    encrypted = False
                elif fd == self.sock.fileno():
                    scheme = "raw"
                    tls = False
                    encrypted = True
                elif fd == self.tls_sock.fileno():
                    scheme = "h2"
                    tls = True
                    encrypted = False
                else:
                    print("bug")
                    continue
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
            except ConnectionAbortedError:
                if conn:
                    conn.close()
                continue
            except Exception as exc:
                self.log.exception(exc)
                if conn:
                    conn.close()
                continue
            try:
                thr = ClientHandler(self, conn, addr, encrypted, scheme, tls, self.tls_context)
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
            mtime = os.path.getmtime(Env.paths.crl)
        except Exception:
            mtime = 0
        change = False
        has_crl = False

        if self.crl_mode == "internal":
            for ca in self.ca:
                ca.unset_lazy("cd")
                if "crl" in ca.data_keys():
                    has_crl = True
                    try:
                        refmtime = os.path.getmtime(ca.paths.cf)
                    except Exception:
                        continue
                    if mtime >= refmtime:
                        continue
                    change = True
                    self.log.info("refresh crl: installed version is %s older than %s", print_duration(refmtime-mtime), ca.path)
            if not has_crl and os.path.exists(Env.paths.crl):
                try:
                    self.log.info("remove %s", Env.paths.crl)
                    os.unlink(Env.paths.crl)
                    change = True
                except Exception as exc:
                    self.log.warning("remove %s: %s", Env.paths.crl, exc)
        elif self.crl_mode == "external":
            if mtime and mtime <= self.crl_expire:
                self.log.info("refresh crl: installed version is expired since %s", print_duration(self.crl_expire-mtime))
                change = True

        if change:
            self.setup_socktls(force=True)

    def janitor_relay(self):
        """
        Purge expired relay.
        """
        now = time.time()
        if now - self.last_relay_janitor < shared.RELAY_JANITOR_INTERVAL:
            return
        self.last_relay_janitor = now
        with shared.RELAY_LOCK:
            for key in [k for k in shared.RELAY_DATA]:
                age = now - shared.RELAY_DATA[key]["updated"]
                if age > shared.RELAY_SLOT_MAX_AGE:
                    self.log.info("drop relay slot %s aged %s", key, print_duration(age))
                    del shared.RELAY_DATA[key]

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
            try:
                event = shared.EVENT_Q.get(False, 0)
            except queue.Empty:
                break
            to_remove = []
            for idx, thr in enumerate(self.events_clients):
                if thr not in self.threads:
                    to_remove.append(idx)
                    continue
                # make a copy, filter_event may change data, avoid being replaced while queued
                fevent = self.filter_event(json.loads(json.dumps(event)), thr)
                if fevent is None:
                    continue
                if thr.h2conn:
                    if not thr.events_stream_ids:
                        to_remove.append(idx)
                        continue
                thr.event_queue.put(fevent)
            for idx in to_remove:
                try:
                    del self.events_clients[idx]
                except IndexError:
                    pass

    def filter_event(self, event, thr):
        if event is None:
            return
        if thr.selector in (None, "**") and (thr.usr is False or "root" in thr.usr_grants):
            # root and no selector => fast path
            return event
        namespaces = thr.get_namespaces()
        kind = event.get("kind")
        if kind == "full":
            return event
        elif kind == "patch":
            return self.filter_patch_event(event, thr, namespaces)
        elif kind == "event":
            return self.filter_event_event(event, thr, namespaces)

    def filter_event_event(self, event, thr, namespaces):
        def valid(change):
            try:
                path = event["data"]["path"]
            except KeyError:
                return True
            if thr.selector and not self.match_object_selector(thr.selector, namespaces=namespaces, path=path):
                return False
            return False
        if valid(event):
            return event
        return None

    def filter_patch_event(self, event, thr, namespaces):
        def filter_change(change):
            try:
                key, value = change
            except:
                key = change[0]
                value = None
            try:
                key_len = len(key)
            except:
                return change
            if key_len == 0:
                if value is None:
                    return change
                value = self.filter_daemon_status(value, namespaces=namespaces, selector=thr.selector)
                return [key, value]
            elif key[0] == "monitor":
                if key_len == 1:
                    if value is None:
                        return change
                    value = self.filter_daemon_status({"monitor": value}, namespaces=namespaces, selector=thr.selector)["monitor"]
                    return [key, value]
                if key[1] == "services":
                    if key_len == 2:
                        if value is None:
                            return change
                        value = dict((k, v) for k, v in value.items() if self.match_object_selector(thr.selector, namespaces=namespaces, path=k))
                        return [key, value]
                    if self.match_object_selector(thr.selector, namespaces=namespaces, path=key[2]):
                        return change
                    else:
                        return
                if key[1] == "nodes":
                    if key_len == 2:
                        if value is None:
                            return change
                        value = self.filter_daemon_status({"monitor": {"nodes": value}}, namespaces=namespaces, selector=thr.selector)["monitor"]["nodes"]
                        return [key, value]
                    if key_len == 3:
                        if value is None:
                            return change
                        value = self.filter_daemon_status({"monitor": {"nodes": {key[2]: value}}}, namespaces=namespaces, selector=thr.selector)["monitor"]["nodes"][key[2]]
                        return [key, value]
                    if key[3] == "services":
                        if key_len == 4:
                            if value is None:
                                return change
                            value = self.filter_daemon_status({"monitor": {"nodes": {key[2]: {"services": value}}}}, namespaces=namespaces, selector=thr.selector)["monitor"]["nodes"][key[2]]["services"]
                            return [key, value]
                        if key[4] == "status":
                            if key_len == 5:
                                if value is None:
                                    return change
                                value = dict((k, v) for k, v in value.items() if self.match_object_selector(thr.selector, namespaces=namespaces, path=k))
                                return [key, value]
                            if self.match_object_selector(thr.selector, namespaces=namespaces, path=key[5]):
                                return change
                            else:
                                return
                        if key[4] == "config":
                            if key_len == 5:
                                if value is None:
                                    return change
                                value = dict((k, v) for k, v in value.items() if self.match_object_selector(thr.selector, namespaces=namespaces, path=k))
                                return [key, value]
                            if self.match_object_selector(thr.selector, namespaces=namespaces, path=key[5]):
                                return change
                            else:
                                return
            return change

        changes = []
        for change in event.get("data", []):
            filtered_change = filter_change(change)
            if filtered_change:
                changes.append(filtered_change)
            #    print("ACCEPT", thr.usr.name if thr.usr else "", filtered_change)
            #else:
            #    print("DROP  ", thr.usr.name if thr.usr else "", change)
        event["data"] = changes
        return event

    def bind_inet(self, sock, addr, port):
        """
        Retry bind until the error is no longer "in use"
        """
        while True:
            if self.stopped():
                break
            try:
                sock.bind((addr, port))
                break
            except socket.error as exc:
                if exc.errno == EADDRINUSE:
                    time.sleep(0.5)
                    continue
                raise

    def setup_socktls(self, force=False):
        self.vip
        if not has_ssl:
            self.log.info("skip tls listener init: ssl module import error")
            return
        port = shared.NODE.oget("listener", "tls_port")
        addr = shared.NODE.oget("listener", "tls_addr")
        if self.tls_port < 0 or not self.tls_addr:
            self.tls_port = port
            self.tls_addr = addr
        elif force or port != self.tls_port or addr != self.tls_addr:
            try:
                self.tls_sock.close()
            except socket.error:
                pass
            try:
                del self.sockmap[self.tls_sock.fileno()]
            except KeyError:
                pass
            self.tls_port = port
            self.tls_addr = addr
        else:
            self.log.info("tls listener %s config unchanged", fmt_listener(self.tls_addr, self.tls_port))
            return

        try:
            addrinfo = socket.getaddrinfo(self.tls_addr, None)[0]
            self.tls_context = self.get_http2_ssl_context()
            self.tls_addr = addrinfo[4][0]
            if ":" in self.tls_addr:
                af = socket.AF_INET6
            else:
                af = socket.AF_INET
            self.tls_sock = socket.socket(af, socket.SOCK_STREAM)
            self.tls_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.bind_inet(self.tls_sock, self.tls_addr, self.tls_port)
            self.tls_sock.listen(128)
            self.tls_sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.alert("error", "bind tls listener %s error: %s", fmt_listener(self.tls_addr, self.tls_port), exc)
            return
        except ex.InitError as exc:
            self.log.info("skip tls listener init: %s", exc)
            return
        except Exception as exc:
            self.log.info("failed tls listener init: %s", exc)
            return
        self.log.info("listening on %s using http/2 tls with client auth", fmt_listener(self.tls_addr, self.tls_port))
        self.sockmap[self.tls_sock.fileno()] = self.tls_sock

    def setup_sock(self):
        port = shared.NODE.oget("listener", "port")
        addr = shared.NODE.oget("listener", "addr")
        if self.port < 0 or not self.addr:
            self.port = port
            self.addr = addr
        elif port != self.port or addr != self.addr:
            try:
                self.sock.close()
            except socket.error:
                pass
            try:
                del self.sockmap[self.sock.fileno()]
            except KeyError:
                pass
            self.port = port
            self.addr = addr
        else:
            self.log.info("aes listener %s config unchanged", fmt_listener(self.addr, self.port))
            return

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            if ":" in self.addr:
                af = socket.AF_INET6
            else:
                af = socket.AF_INET
            self.sock = socket.socket(af, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.bind_inet(self.sock, self.addr, self.port)
            self.sock.listen(128)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.alert("error", "bind aes listener %s error: %s", fmt_listener(self.addr, self.port), exc)
            return
        self.log.info("listening on %s using aes encryption", fmt_listener(self.addr, self.port))
        self.sockmap[self.sock.fileno()] = self.sock

    def setup_sockux_h2(self):
        if os.name == "nt":
            return
        if self.sockuxh2:
            self.log.info("raw listener %s config unchanged", Env.paths.lsnruxsock)
            return
        if not os.path.exists(Env.paths.lsnruxsockd):
            os.makedirs(Env.paths.lsnruxsockd)
        try:
            if os.path.isdir(Env.paths.lsnruxh2sock):
                shutil.rmtree(Env.paths.lsnruxh2sock)
            else:
                os.unlink(Env.paths.lsnruxh2sock)
        except Exception:
            pass
        try:
            self.sockuxh2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockuxh2.bind(Env.paths.lsnruxh2sock)
            self.sockuxh2.listen(1)
            self.sockuxh2.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.alert("error", "bind http/2 listener %s error: %s", Env.paths.lsnruxh2sock, exc)
            return
        self.log.info("listening on %s using http/2", Env.paths.lsnruxh2sock)
        self.sockmap[self.sockuxh2.fileno()] = self.sockuxh2

    def setup_sockux(self):
        if os.name == "nt":
            return
        if self.sockux:
            self.log.info("raw listener %s config unchanged", Env.paths.lsnruxsock)
            return
        if not os.path.exists(Env.paths.lsnruxsockd):
            os.makedirs(Env.paths.lsnruxsockd)
        try:
            if os.path.isdir(Env.paths.lsnruxsock):
                shutil.rmtree(Env.paths.lsnruxsock)
            else:
                os.unlink(Env.paths.lsnruxsock)
        except Exception:
            pass
        try:
            self.sockux = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockux.bind(Env.paths.lsnruxsock)
            self.sockux.listen(1)
            self.sockux.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.alert("error", "bind raw listener %s error: %s", Env.paths.lsnruxsock, exc)
            return
        self.log.info("listening on %s", Env.paths.lsnruxsock)
        self.sockmap[self.sockux.fileno()] = self.sockux

    def setup_socks(self):
        self.setup_socktls()
        self.setup_sock()
        self.setup_sockux()
        self.setup_sockux_h2()


class ClientHandler(shared.OsvcThread):
    sock_tmo = 5.0
    name = "listener client"

    def __init__(self, parent, conn, addr, encrypted, scheme, tls, tls_context):
        shared.OsvcThread.__init__(self)
        self.parent = parent
        self.event_queue = None
        self.conn = conn
        self.addr = addr
        self.encrypted = encrypted
        self.scheme = scheme
        self.tls = tls
        self.tls_context = tls_context
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.listener"), {"node": Env.nodename, "component": "%s/%s" % (self.parent.name, addr[0])})
        self.streams = {}
        self.h2conn = None
        self.events_stream_ids = []
        self.usr_cf_sum = None
        self.same_auth = lambda h: False
        if scheme == "raw":
            self.usr = False
            self.usr_auth = "secret"
            self.usr_grants = {"root": None}
        else:
            self.usr = None
            self.usr_auth = None
            self.usr_grants = {}
        self.events_counter = 0

    def __str__(self):
        try:
            progress = self.parent.stats.sessions.alive[self.sid].progress
        except Exception:
            progress = "unknown"
        return "client handler thread (client addr: %s, usr: %s, auth: %s, scheme: %s, progress: %s)" % (
            self.addr[0],
            self.usr.name if self.usr else self.usr,
            self.usr_auth,
            self.scheme,
            progress,
        )

    def run(self):
        try:
            close = True
            self.sid = str(uuid.uuid4())
            self.parent.stats.sessions.alive[self.sid] = Storage({
                "created": time.time(),
                "addr": self.addr[0],
                "encrypted": self.encrypted,
                "progress": "init",
            })
            if self.scheme == "h2":
                self.handle_h2_client()
            else:
                self.handle_raw_client()
        except Close:
            pass
        except DontClose:
            close = False
        except (OSError, socket.error) as exc:
            if exc.errno in (0, ECONNRESET):
                pass
        except RuntimeError as exc:
            self.log.error("%s", exc)
        except Exception as exc:
            try:
                ignore = exc.errno == 0
            except AttributeError:
                ignore = False
            if not ignore:
                self.log.error("unexpected: %s", exc)
                traceback.print_exc()
        finally:
            if close:
                del self.parent.stats.sessions.alive[self.sid]
                if self.h2conn:
                    self.h2conn.close_connection()
                self.conn.close()

    def negotiate_tls(self):
        """
        Given an established TCP connection and a HTTP/2-appropriate TLS context,
        this function:
        1. wraps TLS around the TCP connection.
        2. confirms that HTTP/2 was negotiated and, if it was not, throws an error.
        """
        if not self.tls:
            self.tls_conn = self.conn
            return

        try:
            self.tls_conn = self.tls_context.wrap_socket(self.conn, server_side=True)
        except OSError as exc:
            if exc.errno in (0, ECONNRESET):
                # 0: client => server after daemon restart
                # ECONNRESET: server => client after daemon restart
                raise
            raise RuntimeError("tls wrap error: %s"%exc)

        # Always prefer the result from ALPN to that from NPN.
        # You can only check what protocol was negotiated once the handshake is
        # complete.
        negotiated_protocol = self.tls_conn.selected_alpn_protocol()
        if negotiated_protocol is None:
            negotiated_protocol = self.tls_conn.selected_npn_protocol()

        if negotiated_protocol != "h2":
            raise RuntimeError("couldn't negotiate h2: %s" % negotiated_protocol)

    def current_usr_cf_sum(self):
        return self.node_data.get(["services", "config", self.usr.path, "csum"], default="unknown")

    def authenticate_client(self, headers):
        if self.usr is False:
            return

        if self.usr and self.usr_cf_sum == self.current_usr_cf_sum():
            # unchanged server side
            if self.same_auth(headers):
                # unchanged request auth
                return

        if self.addr[0] == "local":
            # only root can talk to the ux sockets
            self.usr = False
            self.usr_auth = "uxsock"
            self.last_auth = None
            self.same_auth = lambda h: True
            return

        secret = headers.get(Headers.secret)
        if secret:
            self.usr = self.authenticate_client_secret(secret)
            self.usr_auth = "secret"
            self.last_auth = secret
            self.same_auth = lambda h: h.get(Headers.secret) == self.last_auth
            return

        authorization = headers.get("authorization")
        if authorization:
            if authorization.startswith("Bearer "):
                self.usr = self.authenticate_client_jwt(authorization)
                self.usr_auth = "jwt"
                self.usr_grants = self.user_grants()
                self.last_auth = authorization
                self.same_auth = lambda h: h.get("authorization") == self.last_auth
                return
            elif authorization.startswith("Basic "):
                self.usr = self.authenticate_client_basic(authorization)
                self.usr_auth = "basic"
                self.usr_grants = self.user_grants()
                self.usr_cf_sum = self.current_usr_cf_sum()
                self.last_auth = authorization
                self.same_auth = lambda h: h.get("authorization") == self.last_auth
                return
        try:
            self.usr = self.authenticate_client_x509()
            self.usr_auth = "x509"
            self.usr_grants = self.user_grants()
            self.usr_cf_sum = self.current_usr_cf_sum()
            #self.log.info("loaded grants for %s, conf %s", self.usr.path, self.usr_cf_sum)
            self.last_auth = None
            self.same_auth = lambda h: h.get(Headers.secret) is None and h.get("authorization") is None
            return
        except Exception as exc:
            #self.log.warning("%s", exc)
            pass

        self.usr = None
        self.usr_grants = {}
        self.last_auth = None
        self.usr_cf_sum = None
        self.same_auth = lambda h: False
        raise ex.Error("refused %s auth" % str(self.addr))

    @lazy
    def jwt_provider_keys(self):
        import requests
        well_known_uri = shared.NODE.oget("listener", "openid_well_known")
        well_known_data = requests.get(well_known_uri).json()
        jwks_uri = well_known_data["jwks_uri"]
        jwks = requests.get(jwks_uri).json()
        keys = dict((k['kid'], RSAAlgorithm.from_jwk(json.dumps(k))) for k in jwks['keys'])
        return keys

    def authenticate_client_basic(self, authorization=None):
        if not authorization:
            raise ex.Error("no authorization header key")
        buff = authorization[6:].strip()
        buff = base64.b64decode(buff)
        name, password = bdecode(buff).split(":", 1)
        usr = factory("usr")(name, namespace="system", volatile=True, log=self.log, node=shared.NODE)
        if not usr.exists():
            raise ex.Error("user %s does not exist" % name)
        if not usr.has_key("password"):
            raise ex.Error("user %s has no password key" % name)
        if password != usr.decode_key("password"):
            raise ex.Error("user %s authentication failed: wrong password" % name)
        return usr

    def authenticate_client_jwt(self, authorization=None):
        if not authorization:
            raise ex.Error("no authorization header key")
        if not has_jwt:
            raise ex.Error("jwt is disabled (import error)")
        token = authorization[7:].strip()
        try:
            header = jwt.get_unverified_header(token)
        except Exception as exc:
            raise ex.Error(str(exc))
        key_id = header['kid']
        algorithm = header['alg']
        public_key = self.jwt_provider_keys[key_id]
        decoded = jwt.decode(token, public_key, audience=self.cluster_name, algorithms=algorithm)
        grant = decoded.get("grant", "")
        if isinstance(grant, list):
            grant = " ".join(grant)
        name = decoded.get("preferred_username")
        if not name:
            name = decoded.get("name", "unknown").replace(" ", "_")
        usr = factory("usr")(name, namespace="system", volatile=True, cd={"DEFAULT": {"grant": grant}}, log=self.log, node=shared.NODE)
        return usr

    def authenticate_client_secret(self, secret=None):
        if not secret:
            raise ex.Error("no secret header key")
        if self.blacklisted(self.addr[0]):
            raise ex.Error("sender %s is blacklisted" % self.addr[0])
        if bdecode(self.cluster_key) == secret:
            # caller will set self.usr to False, meaning superuser
            return False
        self.blacklist(self.addr[0])
        raise ex.Error("wrong secret")

    def authenticate_client_x509(self):
        try:
            cert = self.tls_conn.getpeercert()
        except Exception as exc:
            raise ex.Error("x509 auth: getpeercert failed")
        if cert is None:
            raise ex.Error("x509 auth: no client certificate received")
        subject = dict(x[0] for x in cert['subject'])
        cn = subject["commonName"]
        if cn.endswith(self.cluster_name) and cn.count(".") == 3:
            # service account
            name, namespace, kind = split_fullname(cn, self.cluster_name)
            usr = factory("usr")(name, namespace=namespace, volatile=True, log=self.log, node=shared.NODE)
        else:
            usr = factory("usr")(cn, namespace="system", volatile=True, log=self.log, node=shared.NODE)
        if not usr or not usr.exists():
            self.conn.close()
            raise ex.Error("x509 auth failed: %s (valid cert, unknown user)" % cn)
        return usr

    def prepare_response(self, stream_id, status, data, content_type="application/json", path=None):
        response_headers = [
            (':status', str(status)),
        ]
        if path:
            response_headers += [(":path", path)]
        response_headers += [
            ('content-type', content_type),
            ('server', 'opensvc-h2-server/1.0')
        ]
        if content_type == "text/event-stream":
            response_headers += [
                ('Cache-Control', 'no-cache'),
                ('Connection', 'keep-alive'),
                ('Transfer-Encoding', 'chunked'),
            ]
        if "json" in content_type:
            if data is None:
                data = {}
            data = json.dumps(data).encode()
        elif isinstance(data, six.string_types):
            data = bencode(data)
        elif data is None:
            data = "".encode()
        if data:
            response_headers += [
                ('content-length', str(len(data))),
            ]
        self.h2conn.send_headers(stream_id, response_headers)
        if stream_id not in self.streams:
            self.streams[stream_id] = {"outbound": b''}
        try:
            self.streams[stream_id]["outbound"] += data
        except TypeError as exc:
            pass
        self.send_outbound(stream_id)

    def can_end_stream(self, stream_id):
        if "request" not in self.streams[stream_id]:
            return True
        if self.streams[stream_id].get("pushers"):
            return False
        datalen = self.streams[stream_id].get("datalen", 0)
        headers = self.streams[stream_id].get("request_headers", {})
        if not headers:
            return True
        expectedlen = int(headers.get("Content-Length", [0])[0])
        if datalen >= expectedlen:
            return True
        return False

    def send_outbound(self, stream_id):
        data = self.streams[stream_id]["outbound"]
        end_stream = self.can_end_stream(stream_id)
        window_size = self.h2conn.local_flow_control_window(stream_id)
        window_size = min(window_size, len(data))
        will_send = data[:window_size]
        will_queue = data[window_size:]

        max_size = self.h2conn.max_outbound_frame_size
        for chunk in chunker(will_send, max_size):
            self.h2conn.send_data(stream_id, data=chunk, end_stream=False)
            data_to_send = self.h2conn.data_to_send()
            self.tls_conn.sendall(data_to_send)
            message_len = len(data_to_send)
            self.parent.stats.sessions.tx += message_len
            self.parent.stats.sessions.clients[self.addr[0]].tx += message_len

        self.streams[stream_id]["outbound"] = will_queue

        if not will_queue and end_stream:
            self.h2conn.end_stream(stream_id)
            self.tls_conn.sendall(self.h2conn.data_to_send())
            self.h2_cleanup_stream(stream_id)

    def h2_push_promise(self, stream_id, path, data, content_type):
        """
        Use to promise web resources to a browser.
        """
        promised_stream_id = self.h2conn.get_next_available_stream_id()
        request_headers = [h for h in self.streams[stream_id]["request"].headers if h[0] != ":path"]
        request_headers.insert(0, (":path", path))
        self.h2conn.push_stream(stream_id, promised_stream_id, request_headers)
        self.prepare_response(promised_stream_id, 200, data, content_type)

    def h2_router(self, stream_id):
        content_type = "application/json"
        stream = self.streams[stream_id]
        req = stream["request"]
        req_data = stream["data"]
        headers = dict((bdecode(a), bdecode(b)) for a, b in req.headers)
        path = headers.get(":path").lstrip("/")
        parsed_path = urlparse(path)
        path = parsed_path.path.strip("/")
        query = parse_qs(parsed_path.query)
        method = headers.get(":method", "GET")
        accept = headers.get("accept", "").split(",")
        if path == "favicon.ico":
            return 200, "image/x-icon", ICON
        elif path in ("", "index.html"):
            return self.index()
        elif path == "index.js":
            return self.index_js()
        elif "text/html" in accept:
            return self.index()
        multiplexed = stream["request_headers"].get(Headers.multiplexed) is not None
        node = stream["request_headers"].get(Headers.node)
        if node is not None:
            # rebuild the selector from split o-node header
            node = ",".join([bdecode(x) for x in stream["request_headers"].get(Headers.node)])
        options = json.loads(bdecode(req_data))
        options.update(dict((k, v if len(v) > 1 else v[0]) for (k, v) in query.items()))
        data = {
            "action": path,
            "method": method,
            "node": node,
            "multiplexed": multiplexed,
            "options": options,
        }
        data = self.update_data_from_path(data)
        try:
            handler = self.get_handler(method, data["action"])
        except ex.HTTP as exc:
            result = {"status": exc.status, "error": exc.msg}
            return exc.status, content_type, result

        try:
            self.authenticate_client(headers)
            self.parent.stats.sessions.auth_validated += 1
            self.parent.stats.sessions.clients[self.addr[0]].auth_validated += 1
        except ex.Error:
            if handler.access:
                status = 401
                result = {"status": status, "error": "Not Authorized"}
                return status, content_type, result

        try:
            result = self.router(None, data, stream_id=stream_id, handler=handler)
            status = 200
        except DontClose:
            raise
        except ex.HTTP as exc:
            status = exc.status
            result = {"status": exc.status, "error": exc.msg}
        except ex.Error as exc:
            status = 400
            result = {"status": status, "error": str(exc)}
        except Exception as exc:
            status = 500
            result = {"status": status, "error": str(exc), "traceback": traceback.format_exc()}
            self.log.exception(exc)
        try:
            content_type = self.streams[stream_id]["content_type"]
        except:
            pass
        self.parent.stats.sessions.alive[self.sid].progress = "sending %s result" % self.parent.stats.sessions.alive[self.sid].progress
        return status, content_type, result

    def h2_window_updated(self, event):
        if event.stream_id:
            try:
                self.send_outbound(event.stream_id)
            except KeyError:
                # stream cleaned up during iteration
                pass
        else:
            for stream_id in [sid for sid in self.streams]:
                try:
                    self.send_outbound(stream_id)
                except KeyError:
                    # stream cleaned up during iteration
                    pass

    def h2_request_received(self, event):
        stream_id = event.stream_id
        if event.stream_ended:
            data = b'{}'
        else:
            data = b''
        self.streams[stream_id] = {
            "request": event,
            "request_headers": HTTPHeaderMap(event.headers),
            "data": data,
            "datalen": 0,
            "stream_ended": False,
            "pushers": [],
            "outbound": b'',
        }
        if event.stream_ended:
            status, content_type, data = self.h2_router(stream_id)
            self.prepare_response(stream_id, status, data, content_type)

    def h2_data_received(self, event):
        self.streams[event.stream_id]["data"] += event.data
        self.streams[event.stream_id]["datalen"] += event.flow_controlled_length
        self.streams[event.stream_id]["stream_ended"] = event.stream_ended
        if not event.stream_ended:
            return
        status, content_type, data = self.h2_router(event.stream_id)
        self.prepare_response(event.stream_id, status, data, content_type)

    def h2_stream_ended(self, event):
        pass

    def h2_stream_reset(self, event):
        self.h2_cleanup_stream(event.stream_id)

    def h2_cleanup_stream(self, stream_id):
        if self.streams[stream_id]["outbound"]:
            return
        try:
            del self.streams[stream_id]
        except KeyError:
            pass
        try:
            self.events_stream_ids.remove(stream_id)
            # the janitor will drop the thread from the relay list if
            # self.events_stream_ids is empty
        except ValueError:
            pass

    def h2_received(self, data):
        if not data:
            return
        try:
            events = self.h2conn.receive_data(data)
        except h2.exceptions.ProtocolError as exc:
            self.log.warning("%s", exc)
            self.stop()
            return
        for event in events:
            if isinstance(event, h2.events.RequestReceived):
                self.h2_request_received(event)
            elif isinstance(event, h2.events.WindowUpdated):
                self.h2_window_updated(event)
            elif isinstance(event, h2.events.DataReceived):
                self.h2_data_received(event)
            elif isinstance(event, h2.events.StreamEnded):
                self.h2_stream_ended(event)
            elif isinstance(event, h2.events.StreamReset):
                self.h2_stream_reset(event)
            elif isinstance(event, h2.events.ConnectionTerminated):
                self.stop()

    def handle_h2_client(self):
        self.negotiate_tls()
        self.tls_conn.settimeout(self.sock_tmo)

        # init h2 connection
        h2config = H2Configuration(client_side=False)
        self.h2conn = H2Connection(config=h2config)
        self.h2conn.initiate_connection()
        try:
            self.tls_conn.sendall(self.h2conn.data_to_send())
        except socket.error as exc:
            if exc.errno == EPIPE:
                # daemon restart with connected clients
                return
            raise

        while True:
            if self.stopped():
                break
            try:
                data = self.tls_conn.recv(65535)
                if not data:
                    break
                self.parent.stats.sessions.rx += len(data)
                self.parent.stats.sessions.clients[self.addr[0]].rx += len(data)
                self.h2_received(data)
            except ssl.SSLError:
                pass
            except socket.timeout:
                pass
            except socket.error as exc:
                if exc.errno in (0, ECONNRESET):
                    continue
                self.log.error("%s", exc)
                return
            except h2.exceptions.StreamClosedError:
                return
            except ConnectionResetError:
                return
            except Exception as exc:
                self.log.error("exit on %s %s", type(exc), exc)
                traceback.print_exc()
                return

            # execute all registered pushers
            pushers_per_stream = [(stream_id, stream.get("pushers", [])) for stream_id, stream in self.streams.items() if stream.get("pushers")]
            for stream_id, pushers in pushers_per_stream:
                for pusher in pushers:
                    fn = pusher.get("fn")
                    args = pusher.get("args", [])
                    kwargs = pusher.get("kwargs", {})
                    if not fn:
                        continue
                    try:
                        getattr(self, fn)(stream_id, *args, **kwargs)
                    except Exception as exc:
                        print(exc)

            data_to_send = self.h2conn.data_to_send()
            if data_to_send:
                self.tls_conn.sendall(data_to_send)

    def handle_raw_client(self):
        chunks = []
        buff_size = 4096
        self.conn.setblocking(False)
        while True:
            if self.stopped():
                break
            ready = select.select([self.conn], [], [self.conn], self.sock_tmo)
            if ready[0]:
                chunk = self.sock_recv(self.conn, buff_size)
            else:
                self.log.warning("timeout waiting for data")
                return
            if ready[2]:
                self.log.debug("exceptional condition on socket")
                return
            self.parent.stats.sessions.rx += len(chunk)
            self.parent.stats.sessions.clients[self.addr[0]].rx += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if six.PY3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        del chunks
        self.handle_raw_client_data(data)

    def handle_raw_client_data(self, data):
        if six.PY3:
            dequ = data == b"dequeue_actions"
        else:
            dequ = data == "dequeue_actions"
        if dequ:
            self.parent.stats.sessions.alive[self.sid].progress = "dequeue_actions"
            p = Popen(Env.om + ["node", 'dequeue_actions'],
                      stdout=None, stderr=None, stdin=None,
                      close_fds=os.name!="nt")
            return

        if self.encrypted:
            clustername, nodename, data = self.decrypt(data, sender_id=self.addr[0])
            if nodename in self.cluster_drpnodes:
                result = {"status": 401, "error": "drp node %s is not allowed to request" % nodename}
                self.raw_send_result(result)
                return
            if clustername != "join" and shared.NODE.oget("cluster", "name", impersonate=nodename) != clustername:
                result = {"status": 401, "error": "node %s is not a cluster %s node" % (nodename, clustername)}
                self.raw_send_result(result)
                return
        else:
            try:
                data = self.msg_decode(data)
            except ValueError:
                pass
            nodename = Env.nodename

        #self.log.info("received %s from %s", str(data), nodename)
        self.parent.stats.sessions.auth_validated += 1
        self.parent.stats.sessions.clients[self.addr[0]].auth_validated += 1
        if data is None:
            return
        try:
            result = self.router(nodename, data)
        except DontClose:
            raise
        except ex.Error as exc:
            result = {"status": 400, "error": str(exc)}
        except ex.HTTP as exc:
            result = {"status": exc.status, "error": exc.msg}
        except Exception as exc:
            result = {"status": 500, "error": str(exc), "traceback": traceback.format_exc()}
            self.log.exception(exc)
        self.raw_send_result(result)

    def raw_send_result(self, result):
        if result is None:
            return
        self.parent.stats.sessions.alive[self.sid].progress = "sending %s result" % self.parent.stats.sessions.alive[self.sid].progress
        self.conn.setblocking(True)
        if self.encrypted:
            message = self.encrypt(result)
        else:
            message = self.msg_encode(result)
        for chunk in chunker(message, 64*1024):
            try:
                self.conn.sendall(chunk)
            except socket.error as exc:
                if exc.errno == EPIPE:
                    self.log.info(exc)
                else:
                    self.log.warning(exc)
                break
        message_len = len(message)
        self.parent.stats.sessions.tx += message_len
        self.parent.stats.sessions.clients[self.addr[0]].tx += message_len

    def log_request(self, msg, nodename, lvl="info", **kwargs):
        """
        Append the request origin to the message logged by the router action"
        """
        if not msg:
            return
        if not self.usr or not self.addr or self.addr[0] == "local":
            origin = "requested by %s" % nodename if nodename else "root via unix socket"
        else:
            origin = "requested by %s@%s" % (self.usr.name, self.addr[0])
        if lvl == "error":
            fn = self.log.error
        if lvl == "warning":
            fn = self.log.warning
        else:
            fn = self.log.info
        fn("%s %s", msg, origin)

    @staticmethod
    def options_path(options, required=True):
        for key in ("path", "svcpath", "svcname"):
            try:
                return options[key]
            except KeyError:
                pass
        if required:
            raise ex.HTTP(400, "object path not set")
        return None

    #########################################################################
    #
    # RBAC
    #
    #########################################################################
    def get_all_ns(self):
        data = set()
        for path in self.list_cluster_paths():
            _, ns, _ = split_path(path)
            if ns is None:
                ns = "root"
            data.add(ns)
        return data

    def get_namespaces(self, role="guest"):
        if self.usr is False or "root" in self.usr_grants:
            return self.get_all_ns()
        else:
            return self.usr_grants.get(role, [])

    def user_grants(self, all_ns=None):
        if self.usr is False or self.tls is False:
            return {"root": None}
        grants = self.usr.oget("DEFAULT", "grant")
        return self.parse_grants(grants, all_ns=all_ns)

    def parse_grants(self, grants, all_ns=None):
        data = {}
        if not grants:
            return data
        if all_ns is None:
            all_ns = self.get_all_ns()
        for _grant in grants.split():
            if ":" in _grant:
                role_sel, ns_sel = _grant.split(":", 1)
                for role in role_sel.split(","):
                    if role not in Env.ns_roles:
                        continue
                    if role not in data:
                        data[role] = set()
                    for ns in ns_sel.split(","):
                        for _ns in all_ns:
                            if _ns is None:
                                _ns = "root"
                            if fnmatch.fnmatch(_ns, ns):
                                data[role].add(_ns)
                                for equiv in Env.roles_equiv.get(role, ()):
                                    if equiv not in data:
                                        data[equiv] = set([_ns])
                                    else:
                                        data[equiv].add(_ns)
            else:
                role = _grant
                if role not in Env.cluster_roles:
                    continue
                if role not in data:
                    data[role] = None
        # make sure all ns roles have a key, to avoid checking the key existance
        for role in Env.ns_roles:
            if role not in data:
                data[role] = set()
        return data

    def rbac_requires(self, namespaces=None, roles=None, action=None, grants=None, path=None, **kwargs):
        if self.usr is False:
            # ux and aes socket are not constrainted by rbac
            return
        if roles is None:
            # world-usable
            return
        if grants is None:
            grants = self.usr_grants
        if "root" in grants:
            return
        if isinstance(namespaces, (list, tuple)):
            namespaces = set([ns if ns is not None else "root" for ns in namespaces])
        elif namespaces == "FROM:path":
            if path is None:
                raise ex.HTTP(400, "handler '%s' rbac access namespaces FROM:path but no path passed" % action)
            namespaces = set([split_path(path)[1] or "root"])
        for role in roles:
            if role not in grants:
                continue
            if role in Env.cluster_roles:
                return

            # namespaced role
            role_namespaces = grants[role]
            if not role_namespaces:
                # empty set
                continue
            if namespaces == "ANY":
                # role granted on at least one namespace
                return
            if not len(namespaces - role_namespaces):
                # role granted on all namespaces
                return
        raise ex.HTTP(403, "Forbidden: handler '%s' requested by user '%s' with "
                           "grants '%s' requires role '%s'" % (
                action,
                self.usr.name if self.usr else self.usr,
                self.format_grants(grants),
                ",".join(roles)
        ))

    @staticmethod
    def format_grants(grants):
        elements = []
        for role, namespaces in grants.items():
            if namespaces is None:
                elements.append(role)
            elif not namespaces:
                pass
            else:
                elements.append("%s:%s" % (role, ",".join([ns if ns is not None else "root" for ns in namespaces])))
        return " ".join(elements)

    #########################################################################
    #
    # Routing and Multiplexing
    #
    #########################################################################
    def get_handler(self, method, pathname):
        try:
            return self.parent.handlers[(method, pathname)]
        except KeyError:
            pass
        raise ex.HTTP(501, "handler %s %s is not supported" % (method, pathname))

    def multiplex(self, node, handler, options, data, original_nodename, action, stream_id=None):
        method = handler.routes[0][0]
        try:
            del data["node"]
        except Exception:
            pass
        data["multiplexed"] = True # prevent multiplex at the peer endpoint
        result = {"nodes": {}, "status": 0}
        path = self.options_path(options, required=False)
        if node == "ANY" and path:
            svcnodes = self.get_service_nodes(path)
            try:
                if Env.nodename in svcnodes:
                    # prefer to not relay, if possible
                    nodenames = [Env.nodename]
                else:
                    nodenames = [svcnodes[0]]
            except IndexError:
                return {"error": "unknown service", "status": 1}
        elif node == "ANY":
            nodenames = [Env.nodename]
        else:
            nodenames = shared.NODE.nodes_selector(node, data=self.nodes_data.get())
            if not nodenames:
                return {"info": "empty node selection", "status": 0}
            if path:
                svcnodes = self.get_service_nodes(path)
                nodenames = [n for n in nodenames if n in svcnodes]

        def do_node(nodename):
            if nodename == Env.nodename:
                try:
                    _result = handler.action(nodename, action=action, options=options, stream_id=stream_id, thr=self)
                except ex.HTTP as exc:
                    status = exc.status
                    _result = {"status": exc.status, "error": exc.msg}
                except ex.Error as exc:
                    status = 400
                    _result = {"status": status, "error": str(exc)}
                except Exception as exc:
                    status = 500
                    _result = {"status": status, "error": str(exc), "traceback": traceback.format_exc()}
                    self.log.exception(exc)
                result["nodes"][nodename] = _result
                try:
                    result["status"] += 1 if _result.get("status") else 0
                except AttributeError:
                    # result is not a dict
                    pass
            else:
                if handler.stream:
                    sp = self.socket_parms("https://"+nodename)
                    client_stream_id, conn, resp = self.h2_daemon_stream_conn(data, sp=sp)
                    self.streams[stream_id]["pushers"].append({
                        "fn": "push_peer_stream",
                        "args": [nodename, client_stream_id, conn, resp],
                    })
                    _result = {}
                else:
                    _result = self.daemon_request(data, server=nodename, silent=True, method=method)
                result["nodes"][nodename] = _result
                try:
                    result["status"] += _result.get("status", 0)
                except AttributeError:
                    # result is not a dict
                    pass

        for nodename in nodenames:
            try:
                do_node(nodename)
            except Exception:
                continue

        if handler.stream:
            return
        return result

    def push_peer_stream(self, stream_id, nodename, client_stream_id, conn, resp):
        if conn._sock.can_read:
            conn._recv_cb(client_stream_id)
        while True:
            for msg in self.h2_daemon_stream_fetch(client_stream_id, conn):
                self.h2_stream_send(stream_id, msg)
            if conn._sock.can_read:
                conn._recv_cb(client_stream_id)
            else:
                break

    def create_multiplex(self, handler, options, data, original_nodename, action, stream_id=None):
        h = {}
        template = options.get("template")
        path = options.get("path")
        if template:
            odata = shared.NODE.svc_conf_from_templ("dummy", None, "svc", template)
        else:
            odata = options.get("data", {})
        for path, svcdata in odata.items():
            nodes = svcdata.get("DEFAULT", {}).get("nodes")
            placement = svcdata.get("DEFAULT", {}).get("placement", "nodes order")
            if nodes:
                nodes = shared.NODE.nodes_selector(nodes, data=self.nodes_data.get())
            else:
                nodes = self.get_service_nodes(path)
            if nodes:
                if Env.nodename in nodes:
                    node = Env.nodename
                else:
                    node = nodes[0]
            else:
                node = Env.nodename
            if node not in h:
                h[node] = {}
            h[node][path] = svcdata
        result = {"nodes": {}, "status": 0}
        for nodename, optdata in h.items():
            _options = {}
            _options.update(options)
            _options["data"] = optdata
            if nodename == Env.nodename:
                _result = handler.action(nodename, action=action, options=_options, stream_id=stream_id, thr=self)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
            else:
                _data = {}
                _data.update(data)
                _data["options"] = _options
                _data["multiplexed"] = True # prevent multiplex at the peer endpoint
                self.log_request("relay create/update %s to %s" % (",".join([p for p in optdata]), nodename), original_nodename)
                _result = self.daemon_post(_data, server=nodename, silent=True)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
        return result

    @staticmethod
    def parse_path(s):
        l = s.split("/")
        path = None
        node = None
        if len(l) == 1:
            return node, path, s

        if l[0] == "node":
            node = l[1]
            action = "/".join(l[2:])
        elif l[0] == "object":
            if l[2] in Env.kinds:
                path = fmt_path(l[3], l[1], l[2])
                action = "/".join(l[4:])
            elif l[1] in Env.kinds:
                path = fmt_path(l[2], None, l[1])
                action = "/".join(l[3:])
            else:
                path = fmt_path(l[1], None, "svc")
                action = "/".join(l[2:])
        elif l[0] == "instance":
            node = l[1]
            if l[3] in Env.kinds:
                path = fmt_path(l[4], l[2], l[3])
                action = "/".join(l[4:])
            elif l[2] in Env.kinds:
                path = fmt_path(l[3], None, l[2])
                action = "/".join(l[4:])
            else:
                path = fmt_path(l[2], None, "svc")
                action = "/".join(l[3:])

        # translate action
        if path:
            action = ROUTED_ACTIONS["object"].get(action)
        elif node:
            action = ROUTED_ACTIONS["node"].get(action)
        else:
            action = s

        return node, path, action

    def update_data_from_path(self, data):
        action = data["action"]
        # url path router
        # ex: nodes/n1/logs => n1, None, node_logs
        node, path, action = self.parse_path(action)
        if action != data["action"]:
            data["action"] = action
        if node:
            data["node"] = node
        if path:
            if "options" in data:
                data["options"]["path"] = path
            else:
                data["options"] = {"path": path}
        return data

    def router(self, nodename, data, stream_id=None, handler=None):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        self.parent.stats.sessions.alive[self.sid]['tid'] = shared.NODE.get_tid()
        if not isinstance(data, dict):
            return {"error": "invalid data format", "status": 1}
        if "action" not in data:
            return {"error": "action not specified", "status": 1}

        if handler is None:
            method = data.get("method")
            action = data["action"].lstrip("/")
            handler = self.get_handler(method, action)
        else:
            method = handler.routes[0][0]
            action = handler.routes[0][1]

        # prepare options, sanitized for use as keywords
        options = {}
        for key, val in data.get("options", {}).items():
            options[str(key)] = val
        #print("addr:", self.addr, "tls:", self.tls, "action:", action, "options:", options)
        self.parent.stats.sessions.alive[self.sid].progress = "%s /%s" % (method, action)

        # validate rbac before multiplexing, before privs escalation
        if hasattr(handler, "rbac"):
            handler.rbac(nodename, action=action, options=options, stream_id=stream_id, thr=self)
        else:
            self.rbac_requires(action=action)

        if action == "create":
            return self.create_multiplex(handler, options, data, nodename, action, stream_id=stream_id)
        node = data.get("node")
        if data.get("multiplexed") or handler.multiplex == "never":
            return handler.action(nodename, action=action, options=options, stream_id=stream_id, thr=self)
        if handler.multiplex == "always" or node:
            return self.multiplex(node, handler, options, data, nodename, action, stream_id=stream_id)
        return handler.action(nodename, action=action, options=options, stream_id=stream_id, thr=self)


    #########################################################################
    #
    # Handlers Helpers
    #
    #########################################################################

    def h2_push_action_events(self, stream_id):
        while True:
            try:
                msg = self.event_queue.get(False, 0)
            except queue.Empty:
                break
            self.h2_stream_send(stream_id, msg)

    def raw_push_action_events(self):
        while True:
            if self.stopped():
                break
            while True:
                try:
                    buff = self.conn.recv(4096)
                except Exception as exc:
                    break
                if not buff:
                    return
            try:
                msg = self.event_queue.get(True, 1)
            except queue.Empty:
                continue

            if self.encrypted:
                msg = self.encrypt(msg)
            else:
                msg = self.msg_encode(msg)

            self.conn.sendall(msg)

    def logskip(self, backlog, logfile):
        skip = 0
        if backlog > 0:
            fsize = os.path.getsize(logfile)
            if backlog > fsize:
                skip = 0
            else:
                skip = fsize - backlog
        return skip

    def _action_logs_open(self, logfile, backlog, obj):
        skip =  self.logskip(backlog, logfile)
        ofile = open(logfile, "r")
        if backlog > 0:
            self.log.debug("send %s log, backlog %d",
                           obj, backlog)
            try:
                ofile.seek(skip)
            except Exception as exc:
                self.log.info(str(exc))
                ofile.seek(0)
        elif backlog < 0:
            self.log.info("send %s log, whole file", obj)
            ofile.seek(0)
        else:
            self.log.info("follow %s log", obj)
            ofile.seek(0, 2)

        if skip:
            # drop first line (that is incomplete as the seek placed the
            # cursor in the middle
            line = ofile.readline()
        return ofile

    def read_file_lines(self, ofile):
        data = []
        buff = ""
        def parse(_buff):
            head, message = _buff.split(" | ", 1)
            date_s, time_s, lvl, meta = head.split(None, 3)
            dt = datetime.datetime.strptime(date_s + " " + time_s, "%Y-%m-%d %H:%M:%S,%f")
            t = time.mktime(dt.timetuple()) + dt.microsecond / 1000000
            d = {
                "t": t,
                "l": lvl,
                "m": message.rstrip().split("\n"),
                "x": {},
            }
            for m in meta.split():
                k, v = m.split(":", 1)
                d["x"][k] = v
            return d

        while True:
            line = ofile.readline()
            if not line:
                break
            if RE_LOG_LINE.match(line):
                if buff:
                    # new msg, push pending buff
                    try:
                        data.append(parse(buff))
                    except ValueError:
                        pass
                buff = line
            else:
                buff += line
        if buff:
            # EOF, push pending buff
            try:
                data.append(parse(buff))
            except ValueError:
                pass
        return data

    def h2_push_logs(self, stream_id, ofile, follow):
        lines = self.read_file_lines(ofile)
        if not follow:
            ofile.close()
            del self.streams[stream_id]["pushers"]
        if lines:
            self.h2_stream_send(stream_id, lines)

    def h2_sse_stream_send(self, stream_id, data):
        self.events_counter += 1
        msg = "id: %d\n" % self.events_counter
        msg += "data: %s\n\n" % json.dumps(data)
        self.streams[stream_id]["outbound"] += msg.encode()
        self.send_outbound(stream_id)

    def h2_stream_send(self, stream_id, data):
        try:
            content_type = self.streams[stream_id]["content_type"]
        except KeyError:
            content_type = None
        if content_type == "text/event-stream":
            self.h2_sse_stream_send(stream_id, data)
            return
        promised_stream_id = self.h2conn.get_next_available_stream_id()
        request_headers = self.streams[stream_id]["request"].headers
        self.h2conn.push_stream(stream_id, promised_stream_id, request_headers)
        self.prepare_response(promised_stream_id, 200, data)

    def load_file(self, path):
        fpath = os.path.join(Env.paths.pathhtml, path)
        with open(fpath, "r") as f:
            buff = f.read()
        return buff

    ##########################################################################
    #
    # App
    #
    ##########################################################################
    def serve_file(self, rpath, content_type):
        try:
            return 200, content_type, self.load_file(rpath)
        except OSError:
            return 404, content_type, "The webapp is not installed."

    def index(self):
        #data = self.load_file("index.js")
        #self.h2_push_promise(stream_id, "/index.js", data, "application/javascript")
        return self.serve_file("index.html", "text/html")

    def index_js(self):
        return self.serve_file("index.js", "application/javascript")

