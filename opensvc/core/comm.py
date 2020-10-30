"""
The daemon encrypted communications primitives.
"""

from __future__ import print_function
import base64
import json
import os
import socket
import threading
import zlib
import time
import select
import sys
from errno import ECONNREFUSED, EPIPE, EBUSY, EALREADY, EAGAIN


DEFAULT_DAEMON_TIMEOUT = 5


class DummyException(Exception):
    pass

try:
    import ssl
    import foreign.h2.connection
    import foreign.hyper as hyper
    from foreign.hyper.common.headers import HTTPHeaderMap
    SSLWantReadError = ssl.SSLWantReadError
    SSLError = ssl.SSLError
    ssl.HAS_ALPN # stack on Attribute error on py <3.5 and <2.7.10
    has_ssl = True
except Exception:
    # consider py <2.7.9 and <3.4.0 does not have ssl (h2 disabled)
    SSLWantReadError = DummyException
    SSLError = DummyException
    has_ssl = False

import foreign.six as six
import foreign.pyaes as pyaes
from env import Env
from utilities.storage import Storage
from utilities.lazy import lazy
from core.contexts import get_context, want_context
from utilities.string import bdecode
import core.exceptions as ex

if six.PY3:
    def to_bytes(x):
        return bytes(x, "utf-8") if not isinstance(x, bytes) else x
else:
    def to_bytes(x):
        return bytes(x) if not isinstance(x, bytes) else x
    ConnectionResetError = DummyException
    ConnectionRefusedError = DummyException

# add ECONNRESET, ENOTFOUND, ESOCKETTIMEDOUT, ETIMEDOUT, EHOSTUNREACH, ECONNREFUSED, ?
RETRYABLE = (
    EAGAIN,
    EBUSY,
    EPIPE,
    EALREADY,
)
SOCK_TMO_REQUEST = 1.0
SOCK_TMO_STREAM = 6.2
PAUSE = 0.2
PING = ".".encode()

# Number of received misencrypted data messages by senders
BLACKLIST = {}

# The maximum number of misencrypted data messages received before refusing
# new messages
BLACKLIST_THRESHOLD = 5

class Headers(object):
    node = "o-node"
    secret = "o-secret"
    multiplexed = "o-multiplexed"

class SockReset(Exception):
    pass


try:
    from Crypto.Cipher import AES
    from Crypto import __version__ as version
    CRYPTO_MODULE = "pycrypto %s" % version

    def _encrypt(message, key, _iv):
        """
        Low level encrypter.
        """
        message = pyaes.util.append_PKCS7_padding(
            zlib.compress(message)
        )
        obj = AES.new(key, AES.MODE_CBC, _iv)
        ciphertext = obj.encrypt(message)
        return ciphertext

    def _decrypt(ciphertext, key, _iv):
        """
        Low level decrypter.
        """
        obj = AES.new(key, AES.MODE_CBC, _iv)
        message = obj.decrypt(ciphertext)
        return zlib.decompress(pyaes.util.strip_PKCS7_padding(message))
except ImportError:
    CRYPTO_MODULE = "fallback"

    def _encrypt(message, key, _iv):
        """
        Low level encrypter.
        """
        obj = pyaes.Encrypter(
            pyaes.AESModeOfOperationCBC(to_bytes(key), iv=_iv)
        )
        ciphertext = obj.feed(zlib.compress(message))
        ciphertext += obj.feed()
        return ciphertext

    def _decrypt(ciphertext, key, _iv):
        """
        Low level decrypter.
        """
        obj = pyaes.Decrypter(
           pyaes.AESModeOfOperationCBC(to_bytes(key), iv=_iv)
        )
        message = obj.feed(ciphertext)
        message += obj.feed()
        return zlib.decompress(message)

def get_http2_client_ssl_context(cafile=None, keyfile=None, certfile=None):
    """
    This function creates an SSLContext object that is suitably configured for
    HTTP/2. If you're working with Python TLS directly, you'll want to do the
    exact same setup as this function does.
    """
    # Get the basic context from the standard library.
    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)
    if keyfile and certfile:
        ctx.load_cert_chain(keyfile=keyfile, certfile=certfile)
    else:
        ctx.load_default_certs()
    ctx.check_hostname = False

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

class Crypt(object):
    """
    A class implement AES encrypt, decrypt and message padding.
    Used by child classes to authenticate senders on data receive.
    """
    def __init__(self):
        self.log = None

    def get_node(self):
        """
        To be redefined by Crypt child classes
        """
        return Storage()

    @staticmethod
    def _encrypt(message, key, _iv):
        """
        A wrapper over the low level encrypter.
        """
        return _encrypt(message, key, _iv)

    @staticmethod
    def _decrypt(ciphertext, key, _iv):
        """
        A wrapper over the low level decrypter.
        """
        return _decrypt(ciphertext, key, _iv)

    @staticmethod
    def gen_iv(urandom=None, locker=None):
        """
        This is 4x faster than calling os.urandom(16) and prevents
        the "too many files open" issue with concurrent access to os.urandom()
        """
        if urandom is None:
            urandom = []
        try:
            return urandom.pop()
        except IndexError:
            locker = locker or threading.RLock()
            try:
                locker.acquire()
                ur = os.urandom(1024)
                urandom += [ur[i:i + 16] for i in range(16, 1024, 16)]
                return ur[0:16]
            finally:
                locker.release()

    @lazy
    def sorted_cluster_nodes(self):
        return sorted(self.cluster_nodes)

    @lazy
    def cluster_nodes(self):
        """
        Return the cluster nodes, read from cluster.nodes in the node
        configuration. If not set, return a list with the local node as the
        only element.
        """
        node = self.get_node()
        if want_context():
            return node._daemon_status()["cluster"]["nodes"]
        nodes = node.oget("cluster", "nodes")

        if nodes:
            if Env.nodename in nodes:
                return nodes
            else:
                nodes.append(Env.nodename)
        else:
            nodes = [Env.nodename]

        from core.objects.ccfg import Ccfg
        svc = Ccfg()
        svc.set_multi(["cluster.nodes=" + " ".join(nodes)], validation=False)
        node.unset_multi(["cluster.nodes"])
        return nodes

    @lazy
    def cluster_drpnodes(self):
        """
        Return the cluster drp nodes, read from cluster.drpnodes in the node
        configuration. If not set, return an empty list.
        """
        node = self.get_node()
        if want_context():
            return node._daemon_status()["cluster"].get("drpnodes", [])
        nodes = node.oget("cluster", "drpnodes")
        return nodes

    @lazy
    def cluster_name(self):
        """
        Return the cluster name, read from cluster.name in the node
        configuration. If not set, return "default".
        """
        node = self.get_node()
        try:
            return node.oget("cluster", "name").lower()
        except Exception as exc:
            pass
        name = "default"
        from core.objects.ccfg import Ccfg
        svc = Ccfg()
        svc.set_multi(["cluster.name=" + name], validation=False)
        return name

    @lazy
    def cluster_names(self):
        node = self.get_node()
        names = set([self.cluster_name])
        for nodename in self.cluster_drpnodes:
            names.add(node.oget("cluster", "name", impersonate=nodename))
        return names

    @lazy
    def cluster_key(self):
        """
        Return the key read from cluster.secret in the node configuration.
        If not already set generate and store a random one.
        """
        node = self.get_node()
        try:
            key = node.oget("cluster", "secret")
            return self.prepare_key(key)
        except Exception as exc:
            pass
        import uuid
        from core.objects.ccfg import Ccfg
        key = uuid.uuid1().hex
        svc = Ccfg()
        svc.set_multi(["cluster.secret="+key], validation=False)
        return self.prepare_key(key)

    @lazy
    def cluster_id(self):
        """
        Return the cluster id read from cluster.id in the node configuration.
        If not already set generate and store a random one.
        """
        node = self.get_node()
        try:
            return node.conf_get("cluster", "id")
        except Exception as exc:
            pass
        import uuid
        from core.objects.ccfg import Ccfg
        cluster_id = str(uuid.uuid1())
        svc = Ccfg()
        svc.set_multi(["cluster.id="+cluster_id], validation=False)
        return cluster_id

    @staticmethod
    def prepare_key(key):
        """
        Return the key in a format expected by the encrypter and decrypter.
        """
        key = key.encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        return key

    def msg_encode(self, data):
        return (json.dumps(data)+'\0').encode()

    def msg_decode(self, message):
        message = bdecode(message).rstrip("\0\x00")
        if len(message) == 0:
            return
        return json.loads(message)

    def decrypt(self, message, cluster_name=None, secret=None, sender_id=None, structured=True):
        """
        Validate the message meta, decrypt and return the data.
        """
        if cluster_name is None:
            cluster_names = self.cluster_names
        else:
            cluster_names = [cluster_name]
        message = bdecode(message).rstrip("\0\x00")
        try:
            message = json.loads(message)
        except ValueError:
            message_len = len(message)
            if message_len > 40:
                self.log.error("misformatted encrypted message from %s: %s",
                               sender_id, message[:30]+"..."+message[-10:])
            elif message_len > 0:
                self.log.error("misformatted encrypted message from %s",
                               sender_id)
            return None, None, None
        msg_clustername = message.get("clustername")
        msg_nodename = message.get("nodename")
        if secret is None:
            if msg_nodename in self.cluster_drpnodes:
                cluster_key = self.get_secret(Storage(server=msg_nodename), None)
            else:
                cluster_key = self.cluster_key
        else:
            cluster_key = secret
        if cluster_name != "join" and \
           msg_clustername not in set(["join"]) | self.cluster_names:
            self.log.warning("discard message from cluster %s, sender %s",
                             msg_clustername, sender_id)
            return None, None, None
        if cluster_key is None:
            return None, None, None
        if msg_nodename is None:
            return None, None, None
        iv = message.get("iv")
        if iv is None:
            return None, None, None
        if self.blacklisted(sender_id):
            return None, None, None
        iv = base64.urlsafe_b64decode(to_bytes(iv))
        data = base64.urlsafe_b64decode(to_bytes(message["data"]))
        try:
            data = self._decrypt(data, cluster_key, iv)
        except Exception as exc:
            self.log.error("decrypt message from %s: %s", msg_nodename, str(exc))
            self.blacklist(sender_id)
            return None, None, None
        if sender_id:
            self.blacklist_clear(sender_id)
        if not structured:
            try:
                loaded = json.loads(bdecode(data))
            except ValueError as exc:
                loaded = data
            if not isinstance(loaded, foreign.six.text_type):
                loaded = data
            return msg_clustername, msg_nodename, loaded
        try:
            return msg_clustername, msg_nodename, json.loads(bdecode(data))
        except ValueError as exc:
            return msg_clustername, msg_nodename, data

    def encrypt(self, data, cluster_name=None, secret=None, encode=True):
        """
        Encrypt and return data in a wrapping structure.
        """
        if cluster_name is None:
            cluster_name = self.cluster_name
        if secret is None:
            cluster_key = self.cluster_key
        else:
            cluster_key = secret
        if cluster_key is None:
            return
        iv = self.gen_iv()
        try:
            data = json.dumps(data).encode()
        except (UnicodeDecodeError, TypeError):
            # already binary data
            pass
        message = {
            "clustername": cluster_name,
            "nodename": Env.nodename,
            "iv": bdecode(base64.urlsafe_b64encode(iv)),
            "data": bdecode(
                base64.urlsafe_b64encode(
                    self._encrypt(data, cluster_key, iv)
                )
            ),
        }
        if encode:
            return (json.dumps(message)+'\0').encode()
        return json.dumps(message)

    def blacklisted(self, sender_id):
        """
        Return True if the sender's problem count is above threshold.
        Else, return False.
        """
        if sender_id is None:
            return False
        sender_id = str(sender_id)
        try:
            count = BLACKLIST[sender_id]
        except Exception:
            count = 0
        if count > BLACKLIST_THRESHOLD:
            self.log.warning("received a message from blacklisted sender %s",
                             sender_id)
            return True
        return False

    def blacklist(self, sender_id):
        """
        Increment the sender's problem count in the blacklist.
        """
        if sender_id is None:
            return
        sender_id = str(sender_id)
        try:
            count = BLACKLIST[sender_id]
        except Exception:
            count = 0
        BLACKLIST[sender_id] = count + 1
        if count == BLACKLIST_THRESHOLD:
            getattr(self, "event")(
                "blacklist_add",
                level="warning",
                data={
                    "sender": sender_id,
                }
            )

    def blacklist_clear(self, sender_id=None):
        """
        Clear the senders blacklist.
        """
        global BLACKLIST
        if sender_id is None and BLACKLIST == {}:
            return
        if sender_id is None:
            BLACKLIST = {}
            self.log.info("blacklist cleared")
        elif sender_id in BLACKLIST:
            del BLACKLIST[sender_id]
            self.log.info("sender %s removed from blacklist" % \
                          sender_id)

    @staticmethod
    def get_blacklist():
        """
        Return a copy of the senders blacklist.
        """
        data = {}
        for key in list(BLACKLIST):
            try:
                data[key] = BLACKLIST[key]
            except Exception:
                pass
        return data

    def get_listener_info(self, nodename):
        """
        Get the listener address and port from node.conf.
        """
        node = self.get_node()
        addr = node.oget("listener", "tls_addr", impersonate=nodename)
        port = node.oget("listener", "tls_port", impersonate=nodename)
        if nodename != Env.nodename and addr in ("0.0.0.0", "::"):
            addr = nodename
            port = 1214
        return addr, port

    def recv_message(self, *args, **kwargs):
        data = self.recv_messages(*args, **kwargs)
        if data is None:
            return
        return data[0]

    @staticmethod
    def sock_recv(sock, bufsize):
        while True:
            try:
                buff = sock.recv(bufsize)
            except SSLError as exc:
                if exc.errno == ssl.SSL_ERROR_WANT_READ:
                    continue
                raise
            break
        return buff

    def recv_messages(self, sock, cluster_name=None, secret=None,
                      use_select=True, encrypted=True, bufsize=65536,
                      stream=False):
        """
        Receive, decrypt and return a message from a socket.
        """
        sock.setblocking(0)
        messages = []
        chunks = []
        sep = b"\x00"
        while True:
            if use_select:
                ready = select.select([sock], [], [sock], 1)
                if ready[0]:
                    chunk = self.sock_recv(sock, bufsize)
                else:
                    raise socket.timeout
                if ready[2]:
                    break
            else:
                chunk = self.sock_recv(sock, bufsize)
            if not chunk:
                if stream:
                    raise SockReset
                break
            if chunk == sep:
                break
            chunks.append(chunk)
        if six.PY3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        if len(data) == 0:
            return
        for message in data.split(sep):
            if encrypted:
                _, _, message = self.decrypt(
                    data, cluster_name=cluster_name, secret=secret
                )
            else:
                message = self.msg_decode(data)
            messages.append(message)
        return messages

    def get_cluster_context(self):
        context = {}
        context["secret"] = True
        cafile = os.path.join(Env.paths.certs, "ca_certificate_chain")
        if os.path.exists(cafile):
            context["cluster"] = {
                "certificate_authority": cafile,
            }
        keyfile = os.path.join(Env.paths.certs, "private_key")
        certfile = os.path.join(Env.paths.certs, "certificate_chain")
        if os.path.exists(keyfile) and os.path.exists(certfile):
            context["user"] = {
                "client_key": keyfile,
                "client_certificate": certfile,
            }
            #context["secret"] = False
        return context

    def socket_parms_ux(self, server):
        if has_ssl:
            return self.socket_parms_ux_h2(server)
        else:
            return self.socket_parms_ux_raw(server)

    def socket_parms_ux_h2(self, server):
        data = Storage()
        data.scheme = "h2"
        data.af = socket.AF_UNIX
        data.to = Env.paths.lsnruxh2sock
        data.encrypted = False
        data.server = server
        data.context = None
        return data

    def socket_parms_ux_raw(self, server):
        data = Storage()
        data.scheme = "raw"
        data.af = socket.AF_UNIX
        data.to = Env.paths.lsnruxsock
        data.encrypted = False
        data.server = server
        data.context = None
        return data

    def socket_parms_from_context(self, server):
        if not has_ssl:
            raise ex.Error("tls1.2 capable ssl module is required but not available")
        data = Storage()
        context = get_context()
        addr = context["cluster"]["addr"]
        port = context["cluster"]["port"]
        data.context = context
        data.scheme = "h2"
        data.af = socket.AF_INET
        data.to = (addr, port)
        data.encrypted = False
        data.tls = True
        data.server = server
        return data

    def socket_parms_parser(self, server):
        data = Storage()

        # defaults
        port = Env.listener_tls_port
        data.scheme = "h2"
        data.tls = True
        data.encrypted = False
        data.server = server
        data.af = socket.AF_INET

        if server.startswith("https://"):
            host = server[8:]
            data.context = self.get_cluster_context()
        elif server.startswith("raw://"):
            data.tls = False
            data.scheme = "raw"
            data.encrypted = True
            port = Env.listener_port
            host = server[6:]
        elif "://" in server:
            scheme, host = server.split("://", 1)
            raise ex.Error("unknown scheme '%s'. use 'raw' or 'https'" % scheme)

        if not host:
            addr = "localhost"
            data.to = (addr, port)
        elif host[0] == "[":
            try:
                # ipv6 notation
                addr, port = host[1:].split("]:", 1)
                port = int(port)
            except:
                addr = host[1:-1]
            data.to = (addr, port)
        else:
            try:
                addr, port = host.split(":", 1)
                port = int(port)
            except:
                addr = host
            data.to = (addr, port)
        return data

    def socket_parms_inet_raw(self, server):
        data = Storage()
        data.server = server
        addr, port = self.get_listener_info(server)
        data.scheme = "raw"
        data.af = socket.AF_INET
        data.to = (addr, port)
        data.encrypted = True
        data.tls = False
        return data

    def socket_parms(self, server=None):
        if os.environ.get("OSVC_ACTION_ORIGIN") != "daemon" and want_context():
            return self.socket_parms_from_context(server)
        if server is None or server == "":
            return self.socket_parms_ux(server)
        if server == Env.nodename and os.name != "nt":
            # Local comms
            return self.socket_parms_ux(server)
        if server == Env.paths.lsnruxsock:
            return self.socket_parms_ux_raw(server)
        if server == Env.paths.lsnruxh2sock:
            return self.socket_parms_ux_h2(server)
        if ":" in server:
            # Explicit server uri (ex: --server https://1.2.3.4:1215)
            return self.socket_parms_parser(server)
        else:
            # relay, arbitrator, node-to-node
            return self.socket_parms_inet_raw(server)

    def get_http2_client_context(self, sp):
        if not sp.tls:
            return
        try:
            cafile = sp.context["cluster"]["certificate_authority"]
        except:
            cafile = None
        try:
            keyfile = sp.context["user"]["client_key"]
            certfile = sp.context["user"]["client_certificate"]
        except:
            keyfile = None
            certfile = None
        return get_http2_client_ssl_context(
            cafile=cafile,
            keyfile=keyfile,
            certfile=certfile,
        )

    def h2c(self, sp=None, **kwargs):
        context = self.get_http2_client_context(sp)
        if isinstance(sp.to, tuple):
            host = sp.to[0]
            port = sp.to[1]
        else:
            host = sp.to
            port = 0
        conn = hyper.HTTP20Connection(host, port=port, ssl_context=context, secure=sp.tls, **kwargs)
        return conn

    def daemon_get(self, *args, **kwargs):
        kwargs["method"] = "GET"
        return self.daemon_request(*args, **kwargs)

    def daemon_post(self, *args, **kwargs):
        kwargs["method"] = "POST"
        return self.daemon_request(*args, **kwargs)

    def daemon_request(self, *args, **kwargs):
        #print("req", args, kwargs)
        #import traceback
        #traceback.print_stack()
        sp = self.socket_parms(kwargs.get("server"))
        if sp.scheme == "raw" and not want_context():
            return self.raw_daemon_request(*args, sp=sp, **kwargs)
        else:
            return self.h2_daemon_request(*args, sp=sp, **kwargs)

    def get_cluster_name(self, sp, cluster_name):
        if want_context():
            return
        if sp.context and not sp.context.get("clustername"):
            return
        elif cluster_name:
            return cluster_name
        elif sp.server in self.cluster_drpnodes:
            node = self.get_node()
            return node.oget("cluster", "name", impersonate=sp.server)
        else:
            return self.cluster_name

    def get_secret(self, sp, secret):
        if want_context():
            return
        if sp.context and not sp.context.get("secret"):
            return
        elif secret:
            return bdecode(secret)
        elif sp.server in self.cluster_drpnodes:
            node = self.get_node()
            return bdecode(self.prepare_key(node.oget("cluster", "secret", impersonate=sp.server)))
        else:
            return bdecode(self.cluster_key)

    def h2_daemon_request(self, data, server=None, node=None, with_result=True, silent=False,
                          cluster_name=None, secret=None, timeout=None, sp=None, method="GET"):
        secret = self.get_secret(sp, secret)
        path = self.h2_path_from_data(data)
        headers = self.h2_headers(node=node, secret=secret, multiplexed=data.get("multiplexed"), af=sp.af)
        body = self.h2_body_from_data(data)
        headers.update({"Content-Length": str(len(body))})
        conn = self.h2c(sp=sp, timeout=timeout)
        elapsed = 0
        while True:
            try:
                conn.request(method, path, headers=headers, body=body)
                break
            except AssertionError as exc:
                raise ex.Error(str(exc))
            except ConnectionResetError:
                return {"status": 1, "error": "%s %s connection reset"%(method, path)}
            except (ConnectionRefusedError, ssl.SSLError, socket.error) as exc:
                try:
                    errno = exc.errno
                except AttributeError:
                    errno = None
                if errno in RETRYABLE and \
                   (timeout == 0 or (timeout and elapsed < timeout)):
                    # Resource temporarily unavailable (busy, overflow)
                    # Retry after a delay, if the daemon is still
                    # running and timeout is not exhausted
                    time.sleep(PAUSE)
                    elapsed += PAUSE
                    continue
                return {"status": 1, "error": "%s"%exc, "errno": errno}
        resp = conn.get_response()
        data = resp.read()
        data = json.loads(bdecode(data))
        return data

    def raw_daemon_request(self, data, server=None, node=None, with_result=True, silent=False,
                           cluster_name=None, secret=None, timeout=None, sp=None, method="GET"):
        """
        Send a request to the daemon running on server and return the result
        fetched if with_result is set.
        """
        elapsed = 0
        sock = None
        if server is None or server == "":
            server = Env.nodename
        if node:
            data["node"] = node
        data["method"] = method
        progress = "connecting"
        try:
            while True:
                try:
                    sp = self.socket_parms(server)
                    secret = self.get_secret(sp, secret)
                    cluster_name = self.get_cluster_name(sp, cluster_name)
                    if sp.af == socket.AF_UNIX:
                        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        sock.settimeout(SOCK_TMO_REQUEST)
                        sock.connect(sp.to)
                    else:
                        sock = socket.create_connection(sp.to, SOCK_TMO_REQUEST)
                    break
                except socket.timeout:
                    elapsed += SOCK_TMO_REQUEST + PAUSE
                    if timeout == 0 or (timeout and elapsed >= timeout):
                        if with_result:
                            return {
                                "status": 1,
                                "err": "timeout daemon request (connect error)",
                            }
                    time.sleep(PAUSE)
                    continue
                except socket.error as exc:
                    if exc.errno in RETRYABLE and \
                       (timeout == 0 or (timeout and elapsed < timeout)):
                        # Resource temporarily unavailable (busy, overflow)
                        # Retry after a delay, if the daemon is still
                        # running and timeout is not exhausted
                        sock.close()
                        time.sleep(PAUSE)
                        elapsed += PAUSE
                        continue
                    raise

            if sp.encrypted:
                message = self.encrypt(data, cluster_name=cluster_name,
                                       secret=secret)
            else:
                message = self.msg_encode(data)
            if message is None:
                return {
                    "status": 1,
                    "err": "failed to encrypt message",
                }

            progress = "sending"
            sock.sendall(message)

            if with_result:
                progress = "receiving"
                elapsed = 0
                while True:
                    try:
                        return self.recv_message(
                            sock, cluster_name=cluster_name, secret=secret,
                            encrypted=sp.encrypted
                        )
                    except socket.timeout:
                        elapsed += SOCK_TMO_REQUEST + PAUSE
                        if timeout == 0 or (timeout and elapsed >= timeout):
                            return {
                                "status": 1,
                                "err": "timeout daemon request (recv_message error)",
                            }
                        time.sleep(PAUSE)
        except socket.error as exc:
            if not silent:
                self.log.error("%s comm error while %s: %s",
                               sp.to, progress, str(exc))
            return {
                "status": 1,
                "error": str(exc),
                "errno": exc.errno,
                "retryable": exc.errno in RETRYABLE,
            }
        finally:
            if sock:
                sock.close()
        return {"status": 0}

    @staticmethod
    def h2_path_from_data(data):
        return "/" + data.get("action", "").lstrip("/")

    def h2_headers(self, node=None, secret=None, multiplexed=None, af=None):
        headers = HTTPHeaderMap()
        if node:
            if isinstance(node, (tuple, list, set)):
                for n in node:
                    headers.update({Headers.node: n})
            else:
                headers.update({Headers.node: node})
        if secret and af != socket.AF_UNIX:
            headers.update({Headers.secret: secret})
        if multiplexed:
            headers.update({Headers.multiplexed: "true"})
        return headers

    @staticmethod
    def h2_body_from_data(data):
        return json.dumps(data.get("options", {})).encode()

    def daemon_stream(self, *args, **kwargs):
        sp = self.socket_parms(kwargs.get("server"))
        if sp.scheme == "h2":
            iterator = self.h2_daemon_stream
        else:
            iterator = self.raw_daemon_stream
        for e in iterator(*args, sp=sp, **kwargs):
            yield e

    def h2_daemon_stream(self, *args, **kwargs):
        while True:
            try:
                for msg in self._h2_daemon_stream(*args, **kwargs):
                    yield msg
            except hyper.common.exceptions.ConnectionResetError:
                time.sleep(PAUSE)
            except socket.error as exc:
                if exc.errno == ECONNREFUSED:
                    time.sleep(PAUSE)
                else:
                    raise

    def _h2_daemon_stream(self, *args, **kwargs):
        stream_id, conn, resp = self.h2_daemon_stream_conn(*args, **kwargs)
        while True:
            for msg in self.h2_daemon_stream_fetch(stream_id, conn):
                yield msg
            conn._recv_cb(stream_id=stream_id)

    def h2_daemon_stream_conn(self, data, server=None, node=None, cluster_name=None, secret=None, sp=None):
        secret = self.get_secret(sp, secret)
        path = self.h2_path_from_data(data)
        headers = self.h2_headers(node=node, secret=secret, multiplexed=data.get("multiplexed"), af=sp.af)
        body = self.h2_body_from_data(data)
        headers.update({"Content-Length": str(len(body))})
        conn = self.h2c(sp=sp, enable_push=True)
        stream_id = conn.request("GET", path, headers=headers, body=body) 
        #data = resp.read()
        resp = conn.get_response(stream_id)
        return stream_id, conn, resp

    def h2_daemon_stream_fetch(self, stream_id, conn):
        resps = []
        for push in conn.get_pushes(stream_id):
            resps.append(push.get_response())
        for resp in resps:
            # resp.read() can modify push.promises_headers, which get_pushes iterates
            # causing a RuntimeError => keep in a separate loop
            evt = resp.read()
            evt = json.loads(bdecode(evt))
            yield evt

    def raw_daemon_stream(self, data, server=None, node=None, cluster_name=None,
                              secret=None, sp=None):
        """
        Send a request to the daemon running on server and yield the results
        fetched if with_result is set.
        """
        if node:
            data["node"] = node
        data["method"] = "GET"
        sp = self.socket_parms(server)
        try:
            if sp.af == socket.AF_UNIX:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(SOCK_TMO_STREAM)
                sock.connect(sp.to)
            else:
                sock = socket.create_connection(sp.to, SOCK_TMO_STREAM)
            if sp.encrypted:
                message = self.encrypt(data, cluster_name=cluster_name,
                                       secret=secret)
            else:
                message = self.msg_encode(data)
            if message is None:
                raise StopIteration()
            sock.sendall(message)
            while True:
                try:
                    data = self.recv_messages(
                        sock, cluster_name=cluster_name, secret=secret,
                        encrypted=sp.encrypted, bufsize=1
                    )
                    if data is None:
                        raise StopIteration()
                    for message in data:
                        yield message
                except socket.timeout:
                    time.sleep(PAUSE)
        except socket.error as exc:
            self.log.error("daemon send to %s error: %s", sp.to, str(exc))
        finally:
            sock.close()

    @staticmethod
    def parse_result(data):
        """
        Extract status and formatted errors from a daemon_get() result.
        Return a (<status>, <error string>) tuple.

        * data format 1:

        {
            "status": 1,
            "error": "foo"
        }

        * data format 2:

        {
            "status": 1,
            "error": ["foo", "bar"]
        }

        * data format 3: multiplexed request

        {
            "status": 1,
            "nodes": {
                "node1": {
                    "error": ["foo", "bar"]
                }
            }
        }
        """
        def _fmt(_data, key, node=None):
            _buff = ""
            if not data or not isinstance(data, dict):
                return _buff
            entries = _data.get(key, [])
            if not isinstance(entries, (list, tuple, set)):
                entries = [entries]
            for entry in entries:
                if node:
                    _buff += "%s: %s\n" % (node, entry)
                else:
                    _buff += "%s\n" % entry
            return _buff

        if data is None:
            return 1, "no data in response", ""
        if not isinstance(data, dict):
            return 1, "unstructured data", ""
        status = data.get("status", 0)
        if "nodes" in data:
            error = ""
            info = ""
            for node, ndata in data["nodes"].items():
                error += _fmt(ndata, "error", node)
                info += _fmt(ndata, "info", node)
            return status, error.rstrip(), info.rstrip()
        error = _fmt(data, "error")
        info = _fmt(data, "info")
        traceback = data.get("traceback")
        if traceback:
            print("Server "+traceback, file=sys.stderr)
        return status, error.rstrip(), info.rstrip()
