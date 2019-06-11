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

class DummyException(Exception):
    pass

try:
    import ssl
    SSLWantReadError = ssl.SSLWantReadError
    SSLError = ssl.SSLError
    has_ssl = True
except Exception:
    SSLWantReadError = DummyException
    SSLError = DummyException
    has_ssl = False

import six
import pyaes
from rcGlobalEnv import rcEnv
from storage import Storage
from rcUtilities import lazy, bdecode
from contexts import get_context, want_context
import rcExceptions as ex

if six.PY3:
    def to_bytes(x):
        return bytes(x, "utf-8") if not isinstance(x, bytes) else x
else:
    def to_bytes(x):
        return bytes(x) if not isinstance(x, bytes) else x

# add ECONNRESET, ENOTFOUND, ESOCKETTIMEDOUT, ETIMEDOUT, ECONNREFUSED, EHOSTUNREACH, EPIPE ?
RETRYABLE = (
    11,		# EBUSY
    146,	# EREFUSED
    149,	# EALREADY
)
SOCK_TMO = 1.0
PAUSE = 0.2
PING = ".".encode()

# Number of received misencrypted data messages by senders
BLACKLIST = {}
BLACKLIST_LOCK = threading.RLock()

# The maximum number of misencrypted data messages received before refusing
# new messages
BLACKLIST_THRESHOLD = 5


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
            zlib.compress(message.encode())
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
        ciphertext = obj.feed(zlib.compress(message.encode()))
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
            if rcEnv.nodename in nodes:
                return nodes
            else:
                nodes.append(rcEnv.nodename)
        else:
            nodes = [rcEnv.nodename]

        from cluster import ClusterSvc
        svc = ClusterSvc()
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
        from cluster import ClusterSvc
        svc = ClusterSvc()
        svc.set_multi(["cluster.name=" + name], validation=False)
        return name

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
        key = uuid.uuid1().hex
        from cluster import ClusterSvc
        svc = ClusterSvc()
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
        cluster_id = str(uuid.uuid1())
        from cluster import ClusterSvc
        svc = ClusterSvc()
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

    def decrypt(self, message, cluster_name=None, secret=None, sender_id=None):
        """
        Validate the message meta, decrypt and return the data.
        """
        if cluster_name is None:
            cluster_name = self.cluster_name
        if secret is None:
            cluster_key = self.cluster_key
        else:
            cluster_key = secret
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
            return None, None
        if cluster_name != "join" and \
           message.get("clustername") not in (cluster_name, "join"):
            self.log.warning("discard message from cluster %s, sender %s",
                             message.get("clustername"), sender_id)
            return None, None
        if cluster_key is None:
            return None, None
        nodename = message.get("nodename")
        if nodename is None:
            return None, None
        iv = message.get("iv")
        if iv is None:
            return None, None
        if self.blacklisted(sender_id):
            return None, None
        iv = base64.urlsafe_b64decode(to_bytes(iv))
        data = base64.urlsafe_b64decode(to_bytes(message["data"]))
        try:
            data = bdecode(self._decrypt(data, cluster_key, iv))
        except Exception as exc:
            self.log.error("decrypt message from %s: %s", nodename, str(exc))
            self.blacklist(sender_id)
            return None, None
        self.blacklist_clear(sender_id)
        try:
            return nodename, json.loads(data)
        except ValueError as exc:
            return nodename, data

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
        message = {
            "clustername": cluster_name,
            "nodename": rcEnv.nodename,
            "iv": bdecode(base64.urlsafe_b64encode(iv)),
            "data": bdecode(
                base64.urlsafe_b64encode(
                    self._encrypt(json.dumps(data), cluster_key, iv)
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
        with BLACKLIST_LOCK:
            count = BLACKLIST.get(sender_id, 0)
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
        with BLACKLIST_LOCK:
            count = BLACKLIST.get(sender_id, 0)
            if sender_id in BLACKLIST:
                BLACKLIST[sender_id] += 1
                if count == BLACKLIST_THRESHOLD:
                    getattr(self, "event")(
                        "blacklist_add",
                        level="warning",
                        data={
                            "sender": sender_id,
                        }
                    )
            else:
                BLACKLIST[sender_id] = 1

    def blacklist_clear(self, sender_id=None):
        """
        Clear the senders blacklist.
        """
        global BLACKLIST
        if sender_id is None and BLACKLIST == {}:
            return
        with BLACKLIST_LOCK:
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
        Return the senders blacklist.
        """
        with BLACKLIST_LOCK:
            return dict(BLACKLIST)

    def get_listener_info(self, nodename):
        """
        Get the listener address and port from node.conf.
        """
        node = self.get_node()
        addr = node.oget("listener", "addr", impersonate=nodename)
        port = node.oget("listener", "port", impersonate=nodename)
        if nodename != rcEnv.nodename and addr == "0.0.0.0":
            addr = nodename
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
                nodename, message = self.decrypt(
                    data, cluster_name=cluster_name, secret=secret
                )
            else:
                message = self.msg_decode(data)
            messages.append(message)
        return messages

    def socket_parms(self, nodename):
        data = Storage()
        data.nodename = nodename
        if os.environ.get("OSVC_ACTION_ORIGIN") != "daemon" and \
           want_context():
            if not has_ssl:
                raise ex.excError("ssl required but not available")
            context = get_context()
            addr = context["cluster"]["addr"]
            port = context["cluster"]["port"]
            data.context = context
            data.af = socket.AF_INET
            data.to = (addr, port)
            data.to_s = "%s:%d" % (addr, port)
            data.encrypted = False
            data.tls = True
        elif nodename == rcEnv.nodename and os.name != "nt":
            data.af = socket.AF_UNIX
            data.to = rcEnv.paths.lsnruxsock
            data.to_s = rcEnv.paths.lsnruxsock
            data.encrypted = False
        else:
            addr, port = self.get_listener_info(nodename)
            data.af = socket.AF_INET
            data.to = (addr, port)
            data.to_s = "%s:%d" % (addr, port)
            data.encrypted = True
        return data

    def daemon_send(self, data, nodename=None, with_result=True, silent=False,
                    cluster_name=None, secret=None, timeout=0):
        """
        Send a request to the daemon running on nodename and return the result
        fetched if with_result is set.
        """
        elapsed = 0
        sock = None
        if nodename is None or nodename == "":
            nodename = rcEnv.nodename
        progress = "connecting"
        try:
            while True:
                try:
                    sp = self.socket_parms(nodename)
                    sock = socket.socket(sp.af, socket.SOCK_STREAM)
                    sock.settimeout(SOCK_TMO)
                    sock.connect(sp.to)
                    break
                except socket.timeout:
                    if timeout > 0 and elapsed > timeout:
                        if with_result:
                            return {
                                "status": 1,
                                "err": "timeout sending request",
                            }
                        time.sleep(PAUSE)
                        elapsed += SOCK_TMO + PAUSE
                        continue
                    raise
                except socket.error as exc:
                    if exc.errno in RETRYABLE and \
                       (timeout == 0 or elapsed < timeout):
                        # Resource temporarily unavailable (busy, overflow)
                        # Retry after a delay, if the daemon is still
                        # running and timeout is not exhausted
                        sock.close()
                        time.sleep(PAUSE)
                        elapsed += PAUSE
                        continue
                    raise

            if sp.tls:
                progress = "wrapping in tls"
                context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=sp.context["cluster"]["certificate_authority"])
                context.load_cert_chain(keyfile=sp.context["user"]["client_key"], certfile=sp.context["user"]["client_certificate"])
                context.check_hostname = False
                sock = context.wrap_socket(sock)
                message = self.msg_encode(data)
            elif sp.encrypted:
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
                        if timeout > 0 and elapsed > timeout:
                            return {
                                "status": 1,
                                "err": "timeout waiting for result",
                            }
                        time.sleep(PAUSE)
                        elapsed += SOCK_TMO + PAUSE
        except socket.error as exc:
            if not silent:
                self.log.error("%s comm error while %s: %s",
                               sp.to_s, progress, str(exc))
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

    def daemon_get_stream(self, data, nodename=None, cluster_name=None,
                          secret=None):
        """
        Send a request to the daemon running on nodename and yield the results
        fetched if with_result is set.
        """
        if nodename in (None, ""):
            nodename = rcEnv.nodename
        sp = self.socket_parms(nodename)
        try:
            sock = socket.socket(sp.af, socket.SOCK_STREAM)
            sock.settimeout(6.2)
            sock.connect(sp.to)
            if sp.tls:
                context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=sp.context["cluster"]["certificate_authority"])
                context.load_cert_chain(keyfile=sp.context["user"]["client_key"], certfile=sp.context["user"]["client_certificate"])
                context.check_hostname = False
                sock = context.wrap_socket(sock)
                message = self.msg_encode(data)
            elif sp.encrypted:
                message = self.encrypt(data, cluster_name=cluster_name,
                                       secret=secret)
            else:
                message = self.msg_encode(data)
            if message is None:
                return
            sock.sendall(message)
            while True:
                data = self.recv_messages(
                    sock, cluster_name=cluster_name, secret=secret,
                    encrypted=sp.encrypted, bufsize=1
                )
                if data is None:
                    return
                for message in data:
                    yield message
        except socket.error as exc:
            self.log.error("daemon send to %s error: %s", sp.to_s, str(exc))
        finally:
            sock.close()

    def daemon_get_streams(self, data, nodenames=None, cluster_name=None,
                           secret=None):
        """
        Send a request to the daemon running on nodename and yield the results
        fetched if with_result is set.
        """
        if nodenames in (None, [None], "", [""]):
            nodenames = [rcEnv.nodename]
        else:
            nodenames = [nodename for nodename in nodenames
                         if nodename not in (None, "")]

        socks = {}
        reconnect = set()

        def init_sock(nodename):
            sp = self.socket_parms(nodename)
            try:
                sock = socket.socket(sp.af, socket.SOCK_STREAM)
                sock.settimeout(6.2)
                sock.connect(sp.to)
                if sp.tls:
                    context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=sp.context["cluster"]["certificate_authority"])
                    context.load_cert_chain(keyfile=sp.context["user"]["client_key"], certfile=sp.context["user"]["client_certificate"])
                    context.check_hostname = False
                    sock = context.wrap_socket(sock)
                    message = self.msg_encode(data)
                elif sp.encrypted:
                    message = self.encrypt(data, cluster_name=cluster_name,
                                           secret=secret)
                else:
                    message = self.msg_encode(data)
                if message is None:
                    return
                sock.sendall(message)
                socks[sock] = sp
                if nodename in reconnect:
                    self.log.debug("reconnected %s", nodename)
                    reconnect.remove(nodename)
            except socket.error as exc:
                self.log.debug("daemon send to %s error: %s",
                               sp.to_s, str(exc))

        for nodename in nodenames:
            init_sock(nodename)

        try:
            while True:
                for nodename in list(reconnect):
                    self.log.debug("reconnect %s", nodename)
                    init_sock(nodename)
                _socks = [sock for sock in socks]
                try:
                    rsock, _, esock = select.select(_socks, [], _socks, 1)
                except Exception as exc:
                    # empty socks
                    break
                for sock in rsock:
                    try:
                        rdata = self.recv_messages(
                            sock, cluster_name=cluster_name, secret=secret,
                            use_select=False, encrypted=socks[sock].encrypted,
                            bufsize=1, stream=True
                        )
                    except SockReset:
                        sp = socks[sock]
                        self.log.debug("lost stream with %s", sp.nodename)
                        sock.close()
                        reconnect.add(sp.nodename)
                        del socks[sock]
                        continue
                    except socket.error as exc2:
                        # [Errno 10054] An existing connection was forcibly
                        #               closed by the remote host
                        if exc2.errno in (104, 10054):
                            # connection reset by peer
                            sp = socks[sock]
                            sock.close()
                            reconnect.add(sp.nodename)
                            del socks[sock]
                            time.sleep(PAUSE)
                            continue
                        raise
                    if rdata is None:
                        continue
                    for message in rdata:
                        yield message
                for sock in esock:
                    del socks[sock]
                if len(socks) == 0:
                    break
                for sock in _socks:
                    if sock in rsock:
                        continue
                    try:
                        sock.send(PING)
                    except Exception as exc:
                        pass
        finally:
            for sock in socks:
                sock.close()

    @staticmethod
    def parse_result(data):
        """
        Extract status and formatted errors from a daemon_send() result.
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
            if not isinstance(data, dict):
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
            return 1, "no data", ""
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

