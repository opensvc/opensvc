"""
Listener Thread
"""
import base64
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
import fnmatch
from subprocess import Popen, PIPE

try:
    import ssl
    import h2.config
    import h2.connection
    from hyper.common.headers import HTTPHeaderMap
    has_ssl = True
except Exception:
    has_ssl = False

import six
import osvcd_shared as shared
import rcExceptions as ex
from six.moves import queue
from rcGlobalEnv import rcEnv
from storage import Storage
from comm import Headers
from rcUtilities import bdecode, drop_option, chunker, svc_pathcf, \
                        split_svcpath, fmt_svcpath, is_service, factory, \
                        makedirs, mimport, set_lazy, lazy, split_fullname, \
                        unset_lazy
from converters import convert_size, print_duration

RELAY_DATA = {}
RELAY_LOCK = threading.RLock()
RELAY_SLOT_MAX_AGE = 24 * 60 * 60
RELAY_JANITOR_INTERVAL = 10 * 60
JANITORS_INTERVAL = 0.5
ICON = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAABigAAAYoBM5cwWAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAJKSURBVDiNbZJLSNRRFMZ/5/5HbUidRSVSuGhMzUKiB9SihYaJQlRSm3ZBuxY9JDRb1NSi7KGGELRtIfTcJBjlItsohT0hjcpQSsM0CMfXzP9xWszM35mpA/dy7+Wc7/vOd67wn9gcuZ8bisa3xG271LXthTdNL/rZ0B0VQbNzA+mX2ra+kL04d86NxY86QpEI8catv0+SIyOMNnr6aa4ba/aylL2cTdVI6tBwrbfUXvKeOXY87Ng2jm3H91dNnWrd++U89kIx7jw48+DMf0bcOtk0MA5gABq6egs91+pRCKc01lXOnG2tn4yAKUYkmWpATDlqevRjdb4PYMWDrSiVqIKCosMX932vAYoQQ8bCgGoVajcDmIau3jxP9bj6/igoFqiTuCeLkDQQQOSEDm3PMQEnfxeqhYlSH6Si6WF4EJjIZE+1AqiGCAZ3GoT1yYcEuSqqMDBacOXMo5JORDJBRJa9V0qMqkiGfHwt1vORlW3ND9ZdB/mZNDANJNmgUXcsnTmx+WCBvuH8G6/GC276BpLmA95XMxvVQdC5NOYkkC8ocG9odRCRzEkI0yzF3pn+SM2SKrfJiCRQYp9uqf9l/p2E3pIdr20DkCvBS6o64tMvtzLTfmTiQlGh05w1iSFyQ23+R3rcsjsqrlPr4X3Q5f6nOw7/iOwpX+wEsyLNwLcIB6TsSQzASon+1n83unbboTtiaczz3FVXD451VG+cawfyEAHPGcdzruPOHpOKp39SdcvzyAqdOh3GsyoBsLxJ1hS+F4l42Xl/Abn0Ctwc5dldAAAAAElFTkSuQmCC")

GUEST_ACTIONS = (
    "eval",
    "get",
    "keys",
    "print_config_mtime",
)
OPERATOR_ACTIONS = (
    "clear",
    "disable",
    "enable",
    "freeze",
    "push_status",
    "push_resinfo",
    "push_config",
    "push_encap_config",
    "presync",
    "prstatus",
    "resource_monitor",
    "restart",
    "resync",
    "run",
    "scale",
    "snooze",
    "start",
    "startstandby",
    "status",
    "stop",
    "stopstandby",
    "thaw",
    "unsnooze",
)
ADMIN_ACTIONS = (
    "add",
    "boot",
    "decode",
    "gen_cert",
    "install",
    "pg_kill",
    "pg_freeze",
    "pg_thaw",
    "provision",
    "run",
    "set_provisioned",
    "set_unprovisioned",
    "shutdown",
    "unprovision",
    "unset",
)


class HTTP(Exception):
    def __init__(self, status, msg=""):
        self.status = status
        self.msg = msg
    def __str__(self):
        return "status %s: %s" % (self.status, self.msg)

class DontClose(Exception):
    pass


class Listener(shared.OsvcThread):
    events_grace_period = True
    sock_tmo = 1.0
    sockmap = {}
    last_janitors = 0
    crl_expire = 0
    crl_mode = None
    tls_sock = None
    tls_context = None

    @lazy
    def certfs(self):
        mod = mimport("res", "fs")
        res = mod.Mount(rid="fs#certs", mount_point=rcEnv.paths.certs, device="tmpfs", fs_type="tmpfs", mount_options="rw,nosuid,nodev,noexec,relatime,size=1m")
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
        if rcEnv.sysname == "Linux" and self.ca and self.cert and self.ca.exists() and self.cert.exists():
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

    def get_http2_ssl_context(self):
        """
        This function creates an SSLContext object that is suitably configured for
        HTTP/2. If you're working with Python TLS directly, you'll want to do the
        exact same setup as this function does.
        """
        ca_cert_chain, cert_chain, private_key, crl = self.prepare_certs()
        # Get the basic context from the standard library.
        ctx = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        #ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.verify_mode = ssl.CERT_OPTIONAL
        ctx.load_cert_chain(cert_chain, keyfile=private_key)
        ctx.load_verify_locations(ca_cert_chain)
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
        unset_lazy(self, "ca")
        unset_lazy(self, "cert")
        unset_lazy(self, "certfs")
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
            try:
                event = shared.EVENT_Q.get(False, 0)
            except queue.Empty:
                break
            emsg = self.encrypt(event)
            msg = self.msg_encode(event)
            to_remove = []
            for idx, thr in enumerate(self.events_clients):
                if thr not in self.threads:
                    to_remove.append(idx)
                    continue
                if thr.h2conn:
                    if not thr.events_stream_ids:
                        to_remove.append(idx)
                        continue
                    _msg = event
                elif thr.encrypted:
                    _msg = emsg
                else:
                    _msg = msg
                thr.event_queue.put(_msg)
            for idx in to_remove:
                try:
                    del self.events_clients[idx]
                except IndexError:
                    pass

    def setup_socktls(self):
        self.vip
        if not has_ssl:
            self.log.info("skip tls listener init: ssl module import error")
            return
        self.tls_port = shared.NODE.oget("listener", "tls_port")
        self.tls_addr = shared.NODE.oget("listener", "tls_addr")
        try:
            self.tls_sock.close()
        except Exception:
            pass
        try:
            addrinfo = socket.getaddrinfo(self.tls_addr, None)[0]
            self.tls_context = self.get_http2_ssl_context()
            self.tls_addr = addrinfo[4][0]
            self.tls_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            self.tls_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tls_sock.bind((self.tls_addr, self.tls_port))
            self.tls_sock.listen(128)
            self.tls_sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.tls_addr, self.tls_port, exc)
            return
        except ex.excInitError as exc:
            self.log.info("skip tls listener init: %s", exc)
            return
        except Exception as exc:
            self.log.info("failed tls listener init: %s", exc)
            return
        self.log.info("listening on %s:%s using http/2 tls with client auth", self.tls_addr, self.tls_port)
        self.sockmap[self.tls_sock.fileno()] = self.tls_sock

    def setup_sock(self):
        self.port = shared.NODE.oget("listener", "port")
        self.addr = shared.NODE.oget("listener", "addr")

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

    def setup_sockux_h2(self):
        if os.name == "nt":
            return
        if not os.path.exists(rcEnv.paths.lsnruxsockd):
            os.makedirs(rcEnv.paths.lsnruxsockd)
        try:
            if os.path.isdir(rcEnv.paths.lsnruxh2sock):
                shutil.rmtree(rcEnv.paths.lsnruxh2sock)
            else:
                os.unlink(rcEnv.paths.lsnruxh2sock)
        except Exception:
            pass
        try:
            self.sockuxh2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockuxh2.bind(rcEnv.paths.lsnruxh2sock)
            self.sockuxh2.listen(1)
            self.sockuxh2.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s error: %s", rcEnv.paths.lsnruxh2sock, exc)
            return
        self.log.info("listening on %s using http/2", rcEnv.paths.lsnruxh2sock)
        self.sockmap[self.sockuxh2.fileno()] = self.sockuxh2

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
        self.setup_sockux_h2()


class ClientHandler(shared.OsvcThread):
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
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener.%s" % addr[0])
        self.streams = {}
        self.h2conn = None
        self.events_stream_ids = []
        self.usr = None
        self.usr_auth = None
        self.events_counter = 0

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
        except DontClose:
            close = False
        except Exception as exc:
            self.log.error("%s", exc)
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
            raise RuntimeError("tls wrap error: %s"%exc)

        # Always prefer the result from ALPN to that from NPN.
        # You can only check what protocol was negotiated once the handshake is
        # complete.
        negotiated_protocol = self.tls_conn.selected_alpn_protocol()
        if negotiated_protocol is None:
            negotiated_protocol = self.tls_conn.selected_npn_protocol()

        if negotiated_protocol != "h2":
            raise RuntimeError("couldn't negotiate h2: %s" % negotiated_protocol)

    def authenticate_client(self, headers):
        if self.usr or self.usr is False:
            return
        if self.addr[0] == "local":
            # only root can talk to the ux sockets
            self.usr = False
            self.usr_auth = "uxsock"
            return
        try:
            self.usr = self.authenticate_client_secret(headers)
            self.usr_auth = "secret"
            return
        except Exception as exc:
            #self.log.warning("%s", exc)
            pass
        try:
            self.usr = self.authenticate_client_x509()
            self.usr_auth = "x509"
            return
        except Exception as exc:
            #self.log.warning("%s", exc)
            pass
        raise ex.excError("refused %s auth" % str(self.addr))

    def authenticate_client_secret(self, headers):
        secret = headers.get(Headers.secret)
        if not secret:
            raise ex.excError("no secret header key")
        if self.blacklisted(self.addr[0]):
            raise ex.excError("sender %s is blacklisted" % self.addr[0])
        if bdecode(self.cluster_key) == secret:
            # caller will set self.usr to False, meaning superuser
            return False
        self.blacklist(self.addr[0])
        raise ex.excError("wrong secret")

    def authenticate_client_x509(self):
        try:
            cert = self.tls_conn.getpeercert()
        except Exception as exc:
            raise ex.excError("x509 auth: getpeercert failed")
        if cert is None:
            raise ex.excError("x509 auth: no client certificate received")
        subject = dict(x[0] for x in cert['subject'])
        cn = subject["commonName"]
        if "." in cn:
            # service account
            name, namespace, kind = split_fullname(cn, self.cluster_name)
            usr = factory("usr")(name, namespace=namespace, volatile=True, log=self.log)
        else:
            usr = factory("usr")(cn, namespace="system", volatile=True, log=self.log)
        if not usr or not usr.exists():
            self.conn.close()
            raise ex.excError("x509 auth failed: %s (valid cert, unknown user)" % cn)
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
            data = data.encode()
        elif data is None:
            data = "".encode()
        if data:
            response_headers += [
                ('content-length', str(len(data))),
            ]
        self.h2conn.send_headers(stream_id, response_headers)
        if stream_id not in self.streams:
            self.streams[stream_id] = {"outbound": b''}
        self.streams[stream_id]["outbound"] += data
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
        try:
            self.authenticate_client(headers)
            self.parent.stats.sessions.auth_validated += 1
            self.parent.stats.sessions.clients[self.addr[0]].auth_validated += 1
        except ex.excError:
            status = 401
            result = {"status": status, "error": "Not Authorized"}
            return status, content_type, result
        path = headers.get(":path").lstrip("/")
        if path == "favicon.ico":
            return 200, "image/x-icon", ICON
        elif path in ("", "index.html"):
            return self.index(stream_id)
        elif path == "index.js":
            return self.index_js()
        elif path == "index.css":
            return self.index_css()
        node = headers.get(Headers.node)
        options = json.loads(bdecode(req_data))
        data = {
            "action": path,
            "node": node,
            "options": options,
        }
        try:
            result = self.router(None, data, stream_id=stream_id)
            status = 200
        except DontClose:
            raise
        except HTTP as exc:
            status = exc.status
            result = {"status": exc.status, "error": exc.msg}
        except ex.excError as exc:
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
        events = self.h2conn.receive_data(data)
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

        # init h2 connection
        h2config = h2.config.H2Configuration(client_side=False)
        self.h2conn = h2.connection.H2Connection(config=h2config)
        self.h2conn.initiate_connection()
        self.tls_conn.sendall(self.h2conn.data_to_send())

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
            except socket.timeout:
                pass
            except h2.exceptions.StreamClosedError:
                return
            except Exception as exc:
                self.log.error("exit on %s %s", type(exc), exc)
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
        self.conn.setblocking(0)
        while True:
            if self.stopped():
                break
            ready = select.select([self.conn], [], [self.conn], 6)
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
            p = Popen([rcEnv.paths.nodemgr, 'dequeue_actions'],
                      stdout=None, stderr=None, stdin=None,
                      close_fds=os.name!="nt")
            return

        if self.encrypted:
            nodename, data = self.decrypt(data, sender_id=self.addr[0])
        else:
            try:
                data = self.msg_decode(data)
            except ValueError:
                pass
            nodename = rcEnv.nodename
        #self.log.info("received %s from %s", str(data), nodename)
        self.parent.stats.sessions.auth_validated += 1
        self.parent.stats.sessions.clients[self.addr[0]].auth_validated += 1
        if data is None:
            return
        try:
            result = self.router(nodename, data)
        except DontClose:
            raise
        except ex.excError as exc:
            result = {"status": 400, "error": str(exc)}
        except HTTP as exc:
            result = {"status": exc.status, "error": exc.msg}
        except Exception as exc:
            result = {"status": 500, "error": str(exc), "traceback": traceback.format_exc()}
        if result:
            self.parent.stats.sessions.alive[self.sid].progress = "sending %s result" % self.parent.stats.sessions.alive[self.sid].progress
            self.conn.setblocking(1)
            if self.encrypted:
                message = self.encrypt(result)
            else:
                message = self.msg_encode(result)
            for chunk in chunker(message, 64*1024):
                try:
                    self.conn.sendall(chunk)
                except socket.error as exc:
                    if exc.errno == 32:
                        # broken pipe
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
            origin = "requested by %s" % nodename
        else:
            origin = "requested by %s@%s" % (self.usr.svcname, self.addr[0])
        if lvl == "error":
            fn = self.log.error
        if lvl == "warning":
            fn = self.log.warning
        else:
            fn = self.log.info
        fn("%s %s", msg, origin)

    #########################################################################
    #
    # RBAC
    #
    #########################################################################
    def get_all_ns(self):
        data = set()
        for path in shared.CLUSTER_DATA[rcEnv.nodename].get("services", {}).get("config", {}):
            _, ns, _ = split_svcpath(path)
            if ns is None:
                ns = "root"
            data.add(ns)
        return data

    def user_grants(self, all_ns=None):
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
                    if role not in rcEnv.ns_roles:
                        continue
                    if role not in data:
                        data[role] = set()
                    for ns in ns_sel.split(","):
                        for _ns in all_ns:
                            if fnmatch.fnmatch(_ns, ns):
                                data[role].add(_ns)
                                for equiv in rcEnv.roles_equiv.get(role, ()):
                                    if equiv not in data:
                                        data[equiv] = set([_ns])
                                    else:
                                        data[equiv].add(_ns)
            else:
                role = _grant
                if role not in rcEnv.cluster_roles:
                    continue
                if role not in data:
                    data[role] = None
        # make sure all ns roles have a key, to avoid checking the key existance
        for role in rcEnv.ns_roles:
            if role not in data:
                data[role] = set()
        return data

    def rbac_requires(self, namespaces=None, roles=None, grants=None, action=None, **kwargs):
        if self.usr is False:
            # ux and aes socket are not constrainted by rbac
            return
        if roles is None:
            roles = ["root"]
        if grants is None:
            grants = self.user_grants()
        if "root" in grants:
            return grants
        if isinstance(namespaces, (list, tuple)):
            namespaces = set(namespaces)
        for role in roles:
            if role not in grants:
                continue
            if role in rcEnv.cluster_roles:
                return grants

            # namespaced role
            role_namespaces = grants[role]
            if not role_namespaces:
                # empty set
                continue
            if namespaces == "ANY":
                # role granted on at least one namespace
                return grants
            if not len(namespaces - role_namespaces):
                # role granted on all namespaces
                return grants
        raise HTTP(403, "Forbidden: handler '%s' requested by user '%s' with grants '%s' requires role '%s'" % (action, self.usr.svcname, self.format_grants(grants), ",".join(roles)))

    @staticmethod
    def format_grants(grants):
        elements = []
        for role, namespaces in grants.items():
            if namespaces is None:
                elements.append(role)
            elif not namespaces:
                pass
            else:
                elements.append("%s:%s" % (role, ",".join(namespaces)))
        return " ".join(elements)

    def rbac_create_data(self, payload=None , **kwargs):
        if self.usr is False:
            return
        if not payload:
            return
        all_ns = self.get_all_ns()
        grants = self.user_grants(all_ns)
        if "root" in grants:
            return []
        errors = []
        for path, cd in payload.items():
            errors += self.rbac_create_obj(path, cd, grants, all_ns, **kwargs)
        return errors

    def rbac_create_obj(self, path, cd, grants, all_ns, **kwargs):
        errors = []
        name, namespace, kind = split_svcpath(path)
        if namespace not in all_ns:
            if namespace == "system":
                errors.append("%s: create the new namespace system requires the root cluster role")
                return errors
            elif "squatter" not in grants:
                errors.append("%s: create the new namespace %s requires the squatter cluster role" % (path, namespace))
                return errors
            elif namespace not in grants["admin"]:
                self.usr.set_multi(["grant+=admin:%s" % namespace])
                grants["admin"].add(namespace)
        self.rbac_requires(roles=["admin"], namespaces=[namespace], grants=grants, **kwargs)
        try:
            orig_obj = factory(kind)(name, namespace=namespace, volatile=True, node=shared.NODE)
        except:
            orig_obj = None
        try:
            obj = factory(kind)(name, namespace=namespace, volatile=True, cd=cd, node=shared.NODE)
        except Exception as exc:
            errors.append("%s: unbuildable config: %s" % (path, exc))
            return errors
        if kind == "vol":
            errors.append("%s: volume create requires the root privilege" % path)
        elif kind == "ccfg":
            errors.append("%s: cluster config create requires the root privilege" % path)
        elif kind == "svc":
            groups = ["disk", "fs", "app", "share", "sync"]
            for r in obj.get_resources(groups):
                if r.rid == "sync#i0":
                    continue
                errors.append("%s: resource %s requires the root privilege" % (path, r.rid))
            for r in obj.get_resources("task"):
                if r.type not in ("task.podman", "task.docker"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
            for r in obj.get_resources("container"):
                if r.type not in ("container.podman", "container.docker"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
            for r in obj.get_resources("ip"):
                if r.type not in ("ip.cni"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
        for section, sdata in cd.items():
            rtype = cd[section].get("type")
            errors += self.rbac_create_data_section(path, section, rtype, sdata, grants, obj, orig_obj, all_ns)
        return errors

    def rbac_create_data_section(self, path, section, rtype, sdata, user_grants, obj, orig_obj, all_ns):
        errors = []
        for key, val in sdata.items():
            if "trigger" in key or key.startswith("pre_") or key.startswith("post_") or key.startswith("blocking_"):
                errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, val))
                continue
            _key = key.split("@")[0]
            try:
                _val = obj.oget(section, _key)
            except Exception as exc:
                errors.append("%s: %s" % (path, exc))
                continue
            # scopable
            for n in obj.nodes | obj.drpnodes:
                _val = obj.oget(section, _key, impersonate=n)
                if _key in ("container_data_dir") and _val:
                    if _val.startswith("/"):
                        errors.append("%s: keyword %s.%s=%s host paths require the root role" % (path, section, key, _val))
                        continue
                if _key in ("devices", "volume_mounts") and _val:
                    _errors = []
                    for __val in _val:
                        if __val.startswith("/"):
                            _errors.append("%s: keyword %s.%s=%s host paths require the root role" % (path, section, key, __val))
                            continue
                    if _errors:
                        errors += _errors
                        break
                if section == "DEFAULT" and _key == "monitor_action" and _val not in ("freezestop", "switch", None):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("container#") and _key == "netns" and _val == "host":
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("container#") and _key == "privileged" and _val not in ("false", False, None):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("ip#") and _key == "netns" and _val in (None, "host"):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
            # unscopable
            if section == "DEFAULT" and _key == "cn":
                errors += self.rbac_kw_cn(path, _val, orig_obj)
            elif section == "DEFAULT" and _key == "grant":
                errors += self.rbac_kw_grant(path, _val, user_grants, all_ns)
        return errors

    def rbac_kw_grant(self, path, val, user_grants, all_ns):
        errors = []
        req_grants = self.parse_grants(val, all_ns)
        for role, namespaces in req_grants.items():
            if namespaces is None:
                # cluster roles
                if role not in user_grants:
                    errors.append("%s: keyword grant=%s requires the %s cluster role" % (path, val, role))
            else:
                # namespaces roles
                delta = set(namespaces) - set(user_grants.get(role, []))
                if delta:
                    delta = sorted(list(delta))
                    errors.append("%s: keyword grant=%s requires the %s:%s privilege" % (path, val, role, ",".join(delta)))
        return errors

    def rbac_kw_cn(self, path, val, orig_obj):
        errors = []
        try:
            orig_cn = orig_obj.oget("DEFAULT", "cn")
        except Exception:
            orig_cn = None
        if orig_cn == val:
            return []
        errors.append("%s: keyword cn=%s requires the root role" % (path, val))
        return errors

    #########################################################################
    #
    # Routing and Multiplexing
    #
    #########################################################################
    def multiplex(self, node, fname, options, data, original_nodename, action, stream_id=None):
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

        if action in ("node_logs", "events", "service_logs"):
            mode = "stream"
        else:
            mode = "get"

        def do_node(nodename):
            if nodename == rcEnv.nodename:
                try:
                    _result = getattr(self, fname)(nodename, action=action, options=options, stream_id=stream_id)
                except HTTP as exc:
                    status = exc.status
                    _result = {"status": exc.status, "error": exc.msg}
                except ex.excError as exc:
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
                if mode == "stream":
                    sp = self.socket_parms("https://"+nodename)
                    client_stream_id, conn, resp = self.h2_daemon_stream_conn(data, sp=sp)
                    self.streams[stream_id]["pushers"].append({
                        "fn": "push_peer_stream",
                        "args": [nodename, client_stream_id, conn, resp],
                    })
                    _result = {}
                else:
                    _result = self.daemon_get(data, server=nodename, silent=True)
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

    def create_multiplex(self, fname, options, data, original_nodename, action, stream_id=None):
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
                _result = getattr(self, fname)(nodename, action=action, options=_options, stream_id=stream_id)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
            else:
                _data = {}
                _data.update(data)
                _data["options"] = _options
                self.log_request("relay create/update %s to %s" % (",".join([p for p in optdata]), nodename), original_nodename)
                _result = self.daemon_get(_data, server=nodename, silent=True)
                result["nodes"][nodename] = _result
                result["status"] += _result.get("status", 0)
        return result

    def router(self, nodename, data, stream_id=None):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        if not isinstance(data, dict):
            return {"error": "invalid data format", "status": 1}
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        action = data["action"]
        fname = "action_" + action
        if not hasattr(self, fname):
            raise HTTP(501, "handler '%s' not supported" % action)
        # prepare options, sanitized for use as keywords
        options = {}
        for key, val in data.get("options", {}).items():
            options[str(key)] = val
        #print("addr:", self.addr, "tls:", self.tls, "action:", action, "options:", options)
        self.parent.stats.sessions.alive[self.sid].progress = fname
        if action == "create":
            return self.create_multiplex(fname, options, data, nodename, action, stream_id=stream_id)
        node = data.get("node")
        if node:
            return self.multiplex(node, fname, options, data, nodename, action, stream_id=stream_id)
        return getattr(self, fname)(nodename, action=action, options=options, stream_id=stream_id)


    #########################################################################
    #
    # Actions
    #
    #########################################################################

    def action_run_done(self, nodename, **kwargs):
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        action = options.get("action")
        rids = options.get("rids")
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
        self.rbac_requires(roles=["heartbeat"], **kwargs)
        options = kwargs.get("options", {})
        cluster_id = options.get("cluster_id", "")
        cluster_name = options.get("cluster_name", "")
        key = "/".join([cluster_id, nodename])
        with RELAY_LOCK:
            RELAY_DATA[key] = {
                "msg": options.get("msg"),
                "updated": time.time(),
                "cluster_name": cluster_name,
                "cluster_id": cluster_id,
                "ipaddr": options.get("addr", [""])[0],
            }
        return {"status": 0}

    def action_relay_rx(self, nodename, **kwargs):
        """
        Serve to <nodename> the relay heartbeat payload emitted by the node in
        <slot>.
        """
        self.rbac_requires(roles=["heartbeat"], **kwargs)
        options = kwargs.get("options", {})
        cluster_id = options.get("cluster_id", "")
        _nodename = options.get("slot")
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
        self.rbac_requires(roles=["heartbeat"], **kwargs)
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
        self.rbac_requires(roles=["blacklistadmin"], **kwargs)
        self.blacklist_clear()
        return {"status": 0}

    def action_daemon_blacklist_status(self, nodename, **kwargs):
        """
        Return the senders blacklist.
        """
        self.rbac_requires(roles=["guest"], namespaces="ANY", **kwargs)
        return {"status": 0, "data": self.get_blacklist()}

    def action_daemon_stats(self, nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        self.rbac_requires(roles=["guest"], namespaces="ANY", **kwargs)
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
        self.rbac_requires(roles=["guest"], namespaces="ANY", **kwargs)
        return {"status": 0, "data": self.nodes_info()}

    def action_daemon_status(self, nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        grants = self.rbac_requires(roles=["guest"], namespaces="ANY", **kwargs)
        options = kwargs.get("options", {})
        data = {
            "pid": shared.DAEMON.pid,
            "cluster": {
                "name": self.cluster_name,
                "id": self.cluster_id,
                "nodes": self.cluster_nodes,
            }
        }
        if grants is None or "root" in grants:
            allowed_namespaces = None
        else:
            allowed_namespaces = grants.get("guest", [])
        with shared.THREADS_LOCK:
            for thr_id, thread in shared.THREADS.items():
                data[thr_id] = thread.status(namespaces=allowed_namespaces, **options)
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
        self.rbac_requires(**kwargs)
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        thr_id = options.get("thr_id")
        if not thr_id:
            self.log_request("stop daemon", nodename, **kwargs)
            if options.get("upgrade"):
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
            if options.get("wait", False):
                with shared.THREADS_LOCK:
                    shared.THREADS[thr_id].join()
        return {"status": 0}

    def action_daemon_start(self, nodename, **kwargs):
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        thr_id = options.get("thr_id")
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        fmt = options.get("format")
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
        options = kwargs.get("options", {})
        fmt = options.get("format")
        if fmt == "json":
            return self._action_get_service_config_json(nodename, **kwargs)
        else:
            return self._action_get_service_config_file(nodename, **kwargs)

    def action_get_secret_key(self, nodename, **kwargs):
        return self.action_get_key(nodename, **kwargs)

    def action_get_key(self, nodename, **kwargs):
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        name, namespace, kind = split_svcpath(svcpath)
        if kind == "cfg":
            role = "guest"
        else:
            # sec, usr
            role = "admin"
        self.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)
        key = options.get("key")
        try:
            return {"status": 0, "data": shared.SERVICES[svcpath].decode_key(key)}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}

    def action_set_key(self, nodename, **kwargs):
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["admin"], namespaces=[namespace], **kwargs)
        key = options.get("key")
        data = options.get("data")
        shared.SERVICES[svcpath].add_key(key, data)
        try:
            return {"status": 0}
        except Exception as exc:
            return {"status": 1, "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_service_config_json(self, nodename, **kwargs):
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if not svcpath:
            return {"error": "no svcpath specified", "status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["admin"], namespaces=[namespace], **kwargs)
        evaluate = options.get("evaluate")
        impersonate = options.get("impersonate")
        try:
            return shared.SERVICES[svcpath].print_config_data(evaluate=evaluate, impersonate=impersonate)
        except Exception as exc:
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_service_config_file(self, nodename, **kwargs):
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if not svcpath:
            return {"error": "no svcpath specified", "status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["admin"], namespaces=[namespace], **kwargs)
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
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath:
            name, namespace, kind = split_svcpath(svcpath)
            self.rbac_requires(roles=["operator"], namespaces=[namespace], **kwargs)
        else:
            self.rbac_requires(roles=["operator"], namespaces="ANY", **kwargs)
        shared.wake_monitor(reason="service %s notification" % svcpath)
        return {"status": 0}

    def action_clear(self, nodename, **kwargs):
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath is None:
            return {"error": "no svcpath specified", "status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["admin"], namespaces=[namespace], **kwargs)
        smon = self.get_service_monitor(svcpath)
        if smon.status.endswith("ing"):
            return {"info": "skip clear on %s instance" % smon.status, "status": 0}
        self.log_request("service %s clear" % svcpath, nodename, **kwargs)
        self.set_smon(svcpath, status="idle", reset_retries=True)
        return {"status": 0, "info": "%s instance cleared" % svcpath}

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
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath is None:
            return {"error": ["no svcpath specified"], "status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        status = options.get("status")
        local_expect = options.get("local_expect")
        global_expect = options.get("global_expect")
        reset_retries = options.get("reset_retries", False)
        stonith = options.get("stonith")
        role = "admin"
        operator = (
            # (local_expect, global_expect, reset_retries)
            (None, None, True),
            (None, "thawed", False),
            (None, "frozen", False),
            (None, "started", False),
            (None, "stopped", False),
            (None, "aborted", False),
            (None, "placed", False),
            (None, "shutdown", False),
        )
        _global_expect = global_expect.split("@")[0] if global_expect else global_expect
        if (local_expect, _global_expect, reset_retries) in operator:
            role = "operator"
        self.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)
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
                              if inst.get("avail") not in ("up", "warn", "n/a") and \
                              inst.get("monitor", {}).get("status") != "started"]
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        status = options.get("status")
        local_expect = options.get("local_expect")
        global_expect = options.get("global_expect")
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        name = options.get("name")
        timeout = options.get("timeout")
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        name = options.get("name")
        lock_id = options.get("id")
        self.lock_release(name, lock_id)
        result = {"status": 0}
        return result

    def action_leave(self, nodename, **kwargs):
        self.rbac_requires(**kwargs)
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
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        args = options.get("args", [])
        kwargs = options.get("kwargs", {})
        shared.COLLECTOR_XMLRPC_QUEUE.insert(0, (args, kwargs))
        result = {
            "status": 0,
        }
        return result

    def action_join(self, nodename, **kwargs):
        self.rbac_requires(**kwargs)
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
        config = shared.NODE.private_cd
        node_section = config.get("node", {})
        cluster_section = config.get("cluster", {})
        if "env" in node_section:
            result["data"]["node"]["data"]["node"]["env"] = shared.NODE.env
        if "nodes" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["nodes"] = " ".join(new_nodes)
        if "name" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["name"] = self.cluster_name
        if "drpnodes" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["drpnodes"] = " ".join(self.cluster_drpnodes)
        if "id" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["id"] = self.cluster_id
        if "quorum" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["quorum"] = self.quorum
        if "dns" in cluster_section:
            result["data"]["node"]["data"]["cluster"]["dns"] = " ".join(shared.NODE.dns)
        for section in config:
            if section.startswith("hb#") or \
               section.startswith("stonith#") or \
               section.startswith("pool#") or \
               section.startswith("network#") or \
               section.startswith("arbitrator#"):
                result["data"]["node"]["data"][section] = config[section]
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
        options = kwargs.get("options", {})
        sync = options.get("sync", True)
        action_mode = options.get("action_mode", True)
        cmd = options.get("cmd")
        action = options.get("action")
        action_options = options.get("options", {})

        if action_options is None:
            action_options = {}

        self.rbac_requires(**kwargs)

        if not cmd and not action:
            self.log_request("node action ('action' not set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }

        for opt in ("node", "server", "daemon"):
            if opt in action_options:
                del action_options[opt]
        if action_mode and action_options.get("local"):
            if "local" in action_options:
                del action_options["local"]
        for opt, ropt in (("jsonpath_filter", "filter"),):
            if opt in action_options:
                action_options[ropt] = action_options[opt]
                del action_options[opt]
        action_options["local"] = True
        pmod = __import__("nodemgr_parser")
        popt = pmod.OPT

        def find_opt(opt):
            for k, o in popt.items():
                if o.dest == opt:
                    return o
                if o.dest == "parm_" + opt:
                    return o

        if cmd:
            cmd = drop_option("--node", cmd, drop_value=True)
            cmd = drop_option("--server", cmd, drop_value=True)
            cmd = drop_option("--daemon", cmd)
            if action_mode and "--local" not in cmd:
                cmd += ["--local"]
        else:
            cmd = [action]
            for opt, val in action_options.items():
                po = find_opt(opt)
                if po is None:
                    continue
                if val == po.default:
                    continue
                if val is None:
                    continue
                opt = po._long_opts[0] if po._long_opts else po._short_opts[0]
                if po.action == "append":
                    cmd += [opt + "=" + str(v) for v in val]
                elif po.action == "store_true" and val:
                    cmd.append(opt)
                elif po.action == "store_false" and not val:
                    cmd.append(opt)
                elif po.type == "string":
                    opt += "=" + val
                    cmd.append(opt)
                elif po.type == "integer":
                    opt += "=" + str(val)
                    cmd.append(opt)
            cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, "nodemgr.py")] + cmd

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
        options = kwargs.get("options", {})
        data = options.get("data")
        if not data:
            return {"status": 0, "info": "no data"}
        errors = self.rbac_create_data(data, **kwargs)
        if errors:
            return {"status": 1, "error": errors}
        sync = options.get("sync", True)
        namespace = options.get("namespace")
        provision = options.get("provision")
        restore = options.get("restore")
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
        Execute a CRM command.
        kwargs.options:
        * svcpath: str
        * action: str
        * options: dict
        * sync: boolean
        * cmd: str (deprecated)
        """
        options = kwargs.get("options", {})
        action = options.get("action")
        cmd = options.get("cmd")
        sync = options.get("sync", True)
        svcpath = options.get("svcpath")
        action_options = options.get("options", {})

        if action_options is None:
            action_options = {}

        role = "root"
        if action in GUEST_ACTIONS:
            role = "guest"
        elif action in OPERATOR_ACTIONS:
            role = "operator"
        elif action in ADMIN_ACTIONS:
            role = "admin"

        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath is None:
            self.log_request("service action (no 'svcpath' set)", nodename, lvl="error", **kwargs)
            return {
                "status": 1,
            }
        name, namespace, kind = split_svcpath(svcpath)
        if action == "set":
            # load current config
            try:
                cf = shared.SERVICES[svcpath].print_config_data()
            except Exception as exc:
                cf = {}
            # purge unwanted sections
            try:
                del cf["metadata"]
            except Exception:
                pass
            for buff in action_options.get("kw", []):
                k, v = buff.split("=", 1)
                if k[-1] in ("+", "-"):
                    k = k[:-1]
                k = k.strip()
                try:
                    s, k = k.split(".", 1)
                except Exception:
                    s = "DEFAULT"
                if s not in cf:
                    cf[s] = {}
                cf[s][k] = v
            payload = {svcpath: cf}
            errors = self.rbac_create_data(payload, **kwargs)
            if errors:
                return {"status": 1, "error": errors}
        else:
            self.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)

        if self.get_service(svcpath) is None and action not in ("create", "deploy"):
            self.log_request("service action (%s not installed)" % svcpath, nodename, lvl="warning", **kwargs)
            return {
                "error": "%s not found" % svcpath,
                "status": 1,
            }
        if not action and not cmd:
            self.log_request("service action (no action set)", nodename, lvl="error", **kwargs)
            return {
                "error": "action not set",
                "status": 1,
            }

        # TODO: rbac on options

        for opt in ("node", "daemon", "svcs", "service", "s", "parm_svcs", "local", "id"):
            if opt in action_options:
                del action_options[opt]
        for opt, ropt in (("jsonpath_filter", "filter"),):
            if opt in action_options:
                action_options[ropt] = action_options[opt]
                del action_options[opt]
        action_options["local"] = True
        pmod = __import__(kind + "mgr_parser")
        popt = pmod.OPT

        def find_opt(opt):
            for k, o in popt.items():
                if o.dest == opt:
                    return o
                if o.dest == "parm_" + opt:
                    return o

        if cmd:
            # compat, requires root
            self.rbac_requires(**kwargs)
        else:
            cmd = [action]
            for opt, val in action_options.items():
                po = find_opt(opt)
                if po is None:
                    continue
                if val == po.default:
                    continue
                if val is None:
                    continue
                opt = po._long_opts[0] if po._long_opts else po._short_opts[0]
                if po.action == "append":
                    cmd += [opt + "=" + str(v) for v in val]
                elif po.action == "store_true" and val:
                    cmd.append(opt)
                elif po.action == "store_false" and not val:
                    cmd.append(opt)
                elif po.type == "string":
                    opt += "=" + val
                    cmd.append(opt)
                elif po.type == "integer":
                    opt += "=" + str(val)
                    cmd.append(opt)

        cmd = rcEnv.python_cmd + [os.path.join(rcEnv.paths.pathlib, kind+"mgr.py"), "-s", svcpath] + cmd
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

    def action_events(self, nodename, stream_id=None, **kwargs):
        self.rbac_requires(roles=["guest"], namespaces="ANY", **kwargs)
        if not self.event_queue:
            self.event_queue = queue.Queue()
        if not self in self.parent.events_clients:
            self.parent.events_clients.append(self)
        if not stream_id in self.events_stream_ids:
            self.events_stream_ids.append(stream_id)
        if self.h2conn:
            request_headers = HTTPHeaderMap(self.streams[stream_id]["request"].headers)
            try:
                content_type = bdecode(request_headers.get("accept").pop())
            except:
                content_type = "application/json"
            self.streams[stream_id]["content_type"] = content_type
            self.streams[stream_id]["pushers"].append({
                "fn": "h2_push_action_events",
            })
        else:
            self.raw_push_action_events()

    def h2_push_action_events(self, stream_id):
        content_type = self.streams[stream_id]["content_type"]
        while True:
            try:
                msg = self.event_queue.get(False, 0)
            except queue.Empty:
                break
            if content_type == "text/event-stream":
                self.h2_sse_stream_send(stream_id, msg)
            else:
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
            self.conn.sendall(msg)

    def action_service_backlogs(self, nodename, stream_id=None, **kwargs):
        """
        Send service past logs.
        kwargs:
        * svcpath
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath is None:
            return {"status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["guest"], namespaces=[namespace], **kwargs)
        svc = self.get_service(svcpath)
        if svc is None:
            return {"status": 1}
        backlog = self.backlog_from_options(options)
        logfile = os.path.join(svc.log_d, svc.svcname+".log")
        ofile = self._action_logs_open(logfile, backlog, "svc")
        return self.read_file_lines(ofile)

    def action_service_logs(self, nodename, stream_id=None, **kwargs):
        """
        Send service logs.
        kwargs:
        * svcpath
        """
        options = kwargs.get("options", {})
        svcpath = options.get("svcpath")
        if not svcpath:
            svcpath = options.get("svcname")
        if svcpath is None:
            return {"status": 1}
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(roles=["guest"], namespaces=[namespace], **kwargs)
        svc = self.get_service(svcpath)
        if svc is None:
            return {"status": 1}
        logfile = os.path.join(svc.log_d, svc.svcname+".log")
        ofile = self._action_logs_open(logfile, 0, "node")
        self.streams[stream_id]["pushers"].append({
            "fn": "h2_push_logs",
            "args": [ofile, True],
        })

    def action_node_backlogs(self, nodename, stream_id=None, **kwargs):
        """
        Send service past logs.
        kwargs:
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        backlog = self.backlog_from_options(options)
        logfile = os.path.join(rcEnv.paths.pathlog, "node.log")
        ofile = self._action_logs_open(logfile, backlog, "node")
        return self.read_file_lines(ofile)

    def action_node_logs(self, nodename, stream_id=None, **kwargs):
        """
        Send node logs.
        kwargs:
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        follow = options.get("follow")
        logfile = os.path.join(rcEnv.paths.pathlog, "node.log")
        ofile = self._action_logs_open(logfile, 0, "node")
        self.streams[stream_id]["pushers"].append({
            "fn": "h2_push_logs",
            "args": [ofile, True],
        })

    def backlog_from_options(self, options):
        backlog = options.get("backlog")
        if backlog is None:
            backlog = 1024 * 10
        else:
            backlog = convert_size(backlog, _to='B')
        return backlog

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
        lines = []
        while True:
            line = ofile.readline()
            if not line:
                return lines
            lines.append(line)
        return lines

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
        promised_stream_id = self.h2conn.get_next_available_stream_id()
        request_headers = self.streams[stream_id]["request"].headers
        self.h2conn.push_stream(stream_id, promised_stream_id, request_headers)
        self.prepare_response(promised_stream_id, 200, data)

    def action_ask_full(self, nodename, **kwargs):
        """
        Reset the gen number of the dataset of a peer node to force him
        to resend a full.
        """
        self.rbac_requires(**kwargs)
        options = kwargs.get("options", {})
        peer = options.get("peer")
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

    def action_container_exec(self, nodename, **kwargs):
        options = Storage(kwargs.get("options", {}))
        svcpath = options.get("svcpath")
        interactive = options.get("interactive", False)
        tty = options.get("tty", False)
        command = options.get("command")
        name, namespace, kind = split_svcpath(svcpath)
        self.rbac_requires(["operator"], namespace=namespace, **kwargs)
        rid = options.get("rid")
        svc = factory(kind)(name, namespace, node=shared.NODE, volatile=True)
        resource = svc.get_resource(rid)
        cmd = resource.exec_cmd(interactive=interactive, tty=tty, command=command)
        #proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)

    def load_file(self, path):
        fpath = os.path.join(rcEnv.paths.pathhtml, path)
        with open(fpath, "r") as f:
            buff = f.read()
        return buff

    def action_whoami(self, nodename, **kwargs):
        data = {
            "name": self.usr.svcname,
            "namespace": self.usr.namespace,
            "auth": self.usr_auth,
            "grant": dict((k, list(v) if v is not None else None) for k, v in self.user_grants().items()),
        }
        return data

    ##########################################################################
    #
    # App
    #
    ##########################################################################
    def index(self, stream_id):
        data = self.load_file("index.js")
        self.h2_push_promise(stream_id, "/index.js", data, "application/javascript")
        data = self.load_file("index.css")
        self.h2_push_promise(stream_id, "/index.css", data, "text/css")
        return 200, "text/html", self.load_file("index.html")

    def index_js(self):
        return 200, "application/javascript", self.load_file("index.js")

    def index_css(self):
        return 200, "text/css", self.load_file("index.css")

